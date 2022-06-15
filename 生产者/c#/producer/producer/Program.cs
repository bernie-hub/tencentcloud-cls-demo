
/*
 * 该demo只提供了最简单的使用方法，具体生产还需要结合调用放来实现
 * 在使用过程中，demo中留的todo项需要替换使用
 *
 * 注意：
 *  1. 该Demo基于Confluent.Kafka/1.8.2版本验证通过
 *  2. MessageMaxBytes最大值不能超过5M
 *  3. 该demo使用同步的方式生产，在使用时也可根据业务场景调整未异步的方式
 *  4. 其他参数在使用过程中可以根据业务参考文档自己调整：https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerConfig.html
 *
 * Confluent.Kafka 参考文档：https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.html
 */


using Confluent.Kafka;

namespace Producer
{
    class Producer
    {
        private static void Main(string[] args)
        {
            var config = new ProducerConfig
            {
                // todo 域名参考 https://cloud.tencent.com/document/product/614/18940#Kafka 填写，注意内网端口9095，公网端口9096
                BootstrapServers = "${domain}:${port}", 
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "${logsetID}", // todo topic所属日志集ID
                SaslPassword = "${SecurityId}#${SecurityKey}", // todo topic所属uin的密钥
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                Acks         = Acks.None, // todo 根据实际使用场景赋值。可取值: Acks.None、Acks.Leader、Acks.All
                MessageMaxBytes = 5242880 // todo 请求消息的最大大小，最大不能超过5M
            };

            // deliveryHandler
            Action<DeliveryReport<Null, string>> handler =
                r => Console.WriteLine(!r.Error.IsError ? $"Delivered message to {r.TopicPartitionOffset}" : $"Delivery Error: {r.Error.Reason}");


            using (var produce = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    // todo 测试验证代码
                    for (var i = 0; i < 100; i++)
                    {
                        // todo 替换日志主题ID
                        produce.Produce("${topicID}", new Message<Null, string> { Value = "C# demo value" }, handler);
                    }
                    produce.Flush(TimeSpan.FromSeconds(10));

                }
                catch (ProduceException<Null, string> pe)
                {
                    Console.WriteLine($"send message receiver error : {pe.Error.Reason}");
                }
            }
        }
    }
}

