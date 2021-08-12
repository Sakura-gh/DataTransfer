using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace DataTransfer
{
    public static class TimeLogger
    {
        private static Stopwatch stopWatch = new Stopwatch();
        private static ILogger logger = null;

        public static void setLogger(ILogger log)
        {
            logger = log;
        }

        public static bool startTimeLogger()
        {
            if (logger == null)
            {
                return false;
            }
            stopWatch.Start();
            return true;
        }

        public static bool stopTimeLogger(string msg = null)
        {
            if (logger == null)
            {
                return false;
            }
            stopWatch.Stop();
            logger.LogInformation(msg + ": spends " + stopWatch.ElapsedMilliseconds + " ms.");
            return true;
        }

        public static bool Log(string msg)
        {
            if (logger == null)
            {
                return false;
            }
            logger.LogInformation(msg);
            return true;
        }
    }
}
