package com.pmw.kafkaproj.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminSimple {

    public final static String TOPIC_NAME = "pmw-topic";

    /*
        Configure AdminClient
    */
    public static AdminClient adminClient() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.152.131:9092");
        AdminClient  adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /*
        Create Topics
    */
    public static void createTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        //replication factor
        NewTopic newTopic = new NewTopic(TOPIC_NAME,1, (short) 1);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        System.out.println("CreateTopicResults:"+ topics);

    }

    /*
        List Topics
    */
    public static void topicLists() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();
        names.stream().forEach(System.out::println);
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.stream().forEach((topicList)->{
                System.out.println(topicList);
        });
    }

    /*
        Delete Topic
     */
    public  static void delTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }


    /*
        Topic Description
        dec:
            (name=pmw-topic,
            internal=false,
            partitions=(partition=0,
                        leader=192.168.152.130:9092
                        (id: 0 rack: null),
                        replicas=192.168.152.130:9092
                        (id: 0 rack: null),
                        isr=192.168.152.130:9092
                        (id: 0 rack: null)),
            authorizedOperations=null)
     */
    public  static void describeTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        entries.stream().forEach((entry)->{
                System.out.println("name: "+entry.getKey()+ ", desc: "+entry.getValue());
        });

    }

    /*
        Topic Configuration
        configResourceConfigResource(type=TOPIC, name='pmw-topic'),
        Config:Config(entries=[ConfigEntry(
                                name=compression.type,
                                value=producer,
                                source=DEFAULT_CONFIG,
                                isSensitive=false,
                                isReadOnly=false, synonyms=[]),
                               ConfigEntry(
                                name=leader.replication.throttled.replicas,
                                value=, source=DEFAULT_CONFIG,
                                isSensitive=false,
                                isReadOnly=false, synonyms=[]),
                               ConfigEntry(name=message.downconversion.enable,
                                value=true, source=DEFAULT_CONFIG,
                                isSensitive=false,
                                isReadOnly=false, synonyms=[]),
                               ConfigEntry(name=min.insync.replicas,
                                value=1, source=DEFAULT_CONFIG,
                                isSensitive=false,
                                isReadOnly=false, synonyms=[]),
                               ConfigEntry(name=segment.jitter.ms,
                                value=0, source=DEFAULT_CONFIG,
                                isSensitive=false, isReadOnly=false, synonyms=[]),
                               ConfigEntry(name=cleanup.policy,
                                value=delete, source=DEFAULT_CONFIG,
                                isSensitive=false,
                                isReadOnly=false, synonyms=[]),
                               ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.format.version, value=2.6-IV0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1048588, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])
     */
    public static void describeConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        configResourceConfigMap.entrySet().stream().forEach((entry)->{
            System.out.println("configResource"+entry.getKey()+ ", Config:"+entry.getValue());
        });

    }

    /*
        Alter Config
     */

    public static void alterConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
//      Map<ConfigResource, Config> configMap = new HashMap<>();
//      ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
//      Config config = new Config(Arrays.asList(new ConfigEntry("preallocate","True")));
//      configMap.put(configResource, config);
//      AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configMap);
//      alterConfigsResult.all().get();

        Map<ConfigResource, Collection<AlterConfigOp>> configMap = new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        AlterConfigOp alterConfigOp = new AlterConfigOp(new ConfigEntry("preallocate","False"), AlterConfigOp.OpType.SET);
        configMap.put(configResource, Arrays.asList(alterConfigOp));
        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(configMap);
        alterConfigsResult.all().get();
    }

    /*
        Increase Partitions
     */
    public static void incrPartitions(int partitions)throws ExecutionException, InterruptedException{
        AdminClient adminClient = adminClient();
        Map<String, NewPartitions> partitionsMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(partitions);
        partitionsMap.put(TOPIC_NAME,newPartitions);
        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(partitionsMap);
        createPartitionsResult.all().get();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//       AdminClient adminClient = AdminSimple.adminClient();
//        System.out.println(adminClient);
//        createTopic();
//        topicLists();
//        delTopics();
//        describeTopics();
//        alterConfig();
//        describeConfig();
//        incrPartitions(2);


    }
}
