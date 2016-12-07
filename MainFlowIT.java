import backtype.storm.Config;  
import backtype.storm.ILocalCluster;  
import backtype.storm.Testing;  
import backtype.storm.generated.StormTopology;  
import backtype.storm.testing.*;  
import backtype.storm.topology.TopologyBuilder;  
import backtype.storm.tuple.Fields;  
import backtype.storm.tuple.Values;  
import org.junit.Assert;  
import org.junit.Test;

import java.io.IOException;  
import java.util.Map;

/**
 * Created by ahmetdal on 18/11/14.
 */
public class MainFlowIT {

    @Test
    public void testMainFlow() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
                    @Override
                    public void run(ILocalCluster cluster) throws IOException {
                        TopologyBuilder builder = new TopologyBuilder();

                        builder.setSpout("fakeKafkaSpout", new FeederSpout(new Fields("field1", "field2")));

                        builder.setBolt("initiator", new LogInititorBolt())
                        .shuffleGrouping("fakeKafkaSpout");

                        builder.setBolt("hashEnrichment", new MyHashEnrichmentBolt())
                        .shuffleGrouping("initiator", "hashEnrichment");

                        StormTopology topology = builder.createTopology();

                        MockedSources mockedSources = new MockedSources();

                          //Our spout will be processing this values.
                        mockedSources.addMockData("fakeKafkaSpout",new Values("fieldValue1", "fieldValue2"));


                        // prepare the config
                        Config conf = new Config();
                        conf.setNumWorkers(2);

                        CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                        completeTopologyParam.setMockedSources(mockedSources);
                        completeTopologyParam.setStormConf(conf);

                        Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

                        // check whether the result is right
                        //Assume that, my initiator bolt is adding "--initiated" as astring at the end of the values.
                        Assert.assertTrue(Testing.multiseteq(new Values(new Values("fieldValue1", "fieldValue2")), Testing.readTuples(result, "fakeKafkaSpout")));

                        Assert.assertTrue(Testing.multiseteq(new Values(new Values("fieldValue1--initiated", "fieldValue2--initiated")), Testing.readTuples(result, "initiator", "hashEnrichment")));
                    }
                }
        );
    }
}
