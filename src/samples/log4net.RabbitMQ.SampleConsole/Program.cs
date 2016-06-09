using System;
using log4net.Config;

[assembly: XmlConfigurator(ConfigFile = "log4net.config", Watch = true)]

namespace log4net.RabbitMQ.SampleConsole
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var log = LogManager.GetLogger(typeof(Program));

            log.Debug("Application start");

            for (int i = 0; i < 10; i++)
            {
                Console.ReadLine();

                log.Debug($"Debug {i}");

                if (i % 2 == 0)
                {
                    try
                    {
                        throw new Exception("dummy exception!!");
                    }
                    catch (Exception ex)
                    {
                        log.Error($"Error {i}: {ex.Message}", ex);
                    }
                }
            }

            log.Debug("Application end");

            LogManager.Shutdown();

            Console.WriteLine("Press enter to exit the application...");
            Console.ReadLine();
        }
    }
}