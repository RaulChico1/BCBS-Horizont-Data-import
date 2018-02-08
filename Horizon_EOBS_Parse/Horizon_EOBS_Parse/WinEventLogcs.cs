using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Horizon_EOBS_Parse
{
    public class WinEventLogcs
    {
        public void WriteEventLogEntry(string message, int eventId, int infoOrError)
        {
            // Create an instance of EventLog
            System.Diagnostics.EventLog eventLog = new System.Diagnostics.EventLog();

            // Check if the event source exists. If not create it.
            if (!System.Diagnostics.EventLog.SourceExists("Cierant_H_Parse"))
            {
                System.Diagnostics.EventLog.CreateEventSource("Cierant_H_Parse", "Application");
            }

            // Set the source name for writing log entries.
            eventLog.Source = "CierantHorizon";



            // Write an entry to the event log.
            if (infoOrError == 0)
            {
                eventLog.WriteEntry(message,
                                    System.Diagnostics.EventLogEntryType.Information,
                                    eventId);
            }
            else
            {
                eventLog.WriteEntry(message,
                                    System.Diagnostics.EventLogEntryType.Error,
                                    eventId);
            }

            // Close the Event Log
            eventLog.Close();
        }
    }
}
