using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using Enyim.Caching.Configuration;
using log4net;

namespace Linq2DynamoDb.DataContext.Tests.Helpers
{
    using StackExchange.Redis;

    public static class RedisController
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(RedisController));

        private static Process _redisServerProcess;
        private static bool? _isLocalRedisServerRequired;

        public static void StartIfRequired()
        {
            if (!IsLocalRedisServerRequired())
            {
                Logger.DebugFormat("Local Redis server is not required.");
                return;
            }

            if (_redisServerProcess != null)
            {
                Logger.DebugFormat("Redis server process is already running with PID: {0}", _redisServerProcess.Id);
                return;
            }

            var fullFilePath = Path.GetFullPath(TestConfiguration.RedisBinaryPath);
            if (!File.Exists(fullFilePath))
            {
                var exception = new FileNotFoundException(
                    string.Format(
                        "Redis server not found at path: '{0}'. Please ensure Redis binary path is correctly pointing to redis-server.exe file",
                        fullFilePath));
                Logger.Error(exception);
                throw exception;
            }

            Logger.DebugFormat("Starting Redis server using binary file found at '{0}'", fullFilePath);
            _redisServerProcess = Process.Start(fullFilePath);

            if (_redisServerProcess == null)
            {
                var exception = new Exception("Redis server could not be started, please verify that binary file is not corrupted");
                Logger.Error(exception);
                throw exception;
            }

            Logger.DebugFormat("Redis server has started");
        }

        public static void Stop()
        {
            if (_redisServerProcess == null)
            {
                Logger.DebugFormat("Memcached server process was not started. Ignoring Stop method call");
                return;
            }

            _redisServerProcess.Kill();

            _redisServerProcess = null;
        }

        private static bool IsLocalRedisServerRequired()
        {
            if (_isLocalRedisServerRequired != null)
            {
                return _isLocalRedisServerRequired.Value;
            }

            try
            {
                var config = new ConfigurationOptions();
                config.EndPoints.Add(TestConfiguration.RedisLocalAddress);
                ConnectionMultiplexer.Connect(config);

                _isLocalRedisServerRequired = false;
            }
            catch
            {
                _isLocalRedisServerRequired = true;
            }
            return _isLocalRedisServerRequired.Value;
        }
    }
}
