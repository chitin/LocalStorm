        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
        mkClusterParam.setDaemonConf(daemonConf);

        com.typesafe.config.Config config = ConfigFactory.load();
        MRRunningJobConfig mrRunningJobConfig = MRRunningJobConfig.newInstance(config);
        List<String> confKeyKeys = makeConfKeyKeys(mrRunningJobConfig);
        mrRunningJobConfig.getZkStateConfig().zkQuorum = zk.getConnectString();
        MRRunningJobParseBolt mrRunningJobParseBolt = new MRRunningJobParseBolt(
                mrRunningJobConfig.getEagleServiceConfig(),
                mrRunningJobConfig.getEndpointConfig(),
                mrRunningJobConfig.getZkStateConfig(),
                confKeyKeys,
                config);



        String spoutName = "fakeMRRunningJobFetchSpout";
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, cluster -> {


                    TopologyBuilder builder = new TopologyBuilder();
                    builder.setSpout(spoutName, new FeederSpout(new Fields("appId", "appInfo", "mrJobEntity")), 1).setNumTasks(1);
                    builder.setBolt("mrRunningJobParseBolt", mrRunningJobParseBolt, 1).setNumTasks(1).fieldsGrouping(spoutName, new Fields("appId"));
                    StormTopology topology = builder.createTopology();
                    MockedSources mockedSources = new MockedSources();
                    InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
                    AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
                    List<AppInfo> appInfos = appsWrapper.getApps().getApp();
                    mockedSources.addMockData(spoutName, new Values(appInfos.get(0).getId(), appInfos.get(0), null));
                    // prepare the config

                    CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                    completeTopologyParam.setMockedSources(mockedSources);
                    Config conf = new Config();
                    conf.setMessageTimeoutSecs(20);
                    conf.setNumWorkers(2);
                    completeTopologyParam.setStormConf(conf);
                    Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

                    Assert.assertTrue(Testing.multiseteq(new Values(new Values(appInfos.get(0).getId(), appInfos.get(0), null)), Testing.readTuples(result, spoutName)));

                }


        );
