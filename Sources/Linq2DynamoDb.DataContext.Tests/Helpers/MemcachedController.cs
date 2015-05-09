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
    public static class MemcachedController
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(MemcachedController));

        private static Process _memcachedServerProcess;
        private static bool? _isLocalMemcachedRequired;

        public static void StartIfRequired()
        {
            if (!IsLocalMemcachedServerRequired())
            {
                Logger.DebugFormat("Local Memcached server is not required.");
                return;
            }

            if (_memcachedServerProcess != null)
            {
                Logger.DebugFormat("Memcached server process is already running with PID: {0}", _memcachedServerProcess.Id);
                return;
            }

            var fullFilePath = Path.GetFullPath(TestConfiguration.MemcachedBinaryPath);
            if (!File.Exists(fullFilePath))
            {
                var exception = new FileNotFoundException(
                    string.Format(
                        "Memcached server not found at path: '{0}'. Please ensure Memcached binary path is correctly pointing to memcached.exe file",
                        fullFilePath));
                Logger.Error(exception);
                throw exception;
            }

            Logger.DebugFormat("Starting memcached server using binary file found at '{0}'", fullFilePath);
            _memcachedServerProcess = Process.Start(fullFilePath);

            if (_memcachedServerProcess == null)
            {
                var exception = new Exception("Memcached server could not be started, please verify that binary file is not corrupted");
                Logger.Error(exception);
                throw exception;
            }

            Logger.DebugFormat("Memcached server has started");
        }

        public static void Stop()
        {
            if (_memcachedServerProcess == null)
            {
                Logger.DebugFormat("Memcached server process was not started. Ignoring Stop method call");
                return;
            }

            _memcachedServerProcess.Kill();

            _memcachedServerProcess = null;
        }

        private static bool IsLocalMemcachedServerRequired()
        {
            if (_isLocalMemcachedRequired != null)
            {
                return _isLocalMemcachedRequired.Value;
            }

            var memcachedSection = (MemcachedClientSection)ConfigurationManager.GetSection("enyim.com/memcached");
            var hasLocalhostServer = memcachedSection.Servers.ToIPEndPointCollection().Any(server => IPAddress.IsLoopback(server.Address));

            _isLocalMemcachedRequired = hasLocalhostServer;
            return _isLocalMemcachedRequired.Value;
        }
    }
}
