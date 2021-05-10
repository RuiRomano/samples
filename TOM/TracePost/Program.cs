using System;
using Microsoft.AnalysisServices.Tabular;
using System.Threading;
using System.Xml;
using Newtonsoft.Json;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;


namespace TracePost
{
    class Program
    {

        static string _sessionId;
        static HttpClient client = new HttpClient();
        static int BatchSleepTime = 5000;
        static int rowsPerPost = 500;
        private const string eventHubConnectionString = "[ENTER EVENTHUB ENDPOINT HERE IF USING STREAMING DATAFLOW]";
        private const string databaseToTrace = "[ENTER CONNECTIONSTRING OF DATABASE TO BE TRACED]";
        // eg. Data source=powerbi://api.powerbi.com/v1.0/myorg/Trace Post Demo;Initial catalog=AdventureWorksDW;
        // eg. asazure://aspaaseastus2.asazure.windows.net/instancenamehere:rw
        private const string eventHubName = "tracepost";
        static string PowerBIAPI = "[ENTER STREAMING DATAFLOW API KEY HERE]";

        static Queue myQ = new Queue();


        static void Main(string[] args)
        {

            Server server = new Server();
            Console.CursorVisible = false;
            Boolean activityFound = false;
            string jsonString;
            string row;
            int rowsProcessed = 0;

            /**************************************************************************
                Connect to Azure Analysis Services or Power BI Premium
                    PBI Premium requires XMLA-Read
                    PBI Premium also requires to connect to Server AND DB (using inital Catalog property)
                    This should enable MFA popup
            **************************************************************************/

            server.Connect(databaseToTrace);
            _sessionId = server.SessionID;

            /**************************************************************************
                Remove any previous version of trace created using this app
            **************************************************************************/

            TraceCollection traceCollection = server.Traces;
            for (int d = 0; d < traceCollection.Count; d++)
            {
                Trace p = traceCollection[d];
                if (p.Name.StartsWith("TracePost"))
                {
                    if (p.IsStarted)
                    { p.Stop(); }
                    p.Drop();
                }
            }

            /**************************************************************************
                Create the trace
            **************************************************************************/

            Trace trace = server.Traces.Add("TracePost");

            trace.AutoRestart = true;

            /**************************************************************************
                Create the event 1 of 2 to trace [ProgressReportCurrent]
            **************************************************************************/

            TraceEvent traceEvent = trace.Events.Add(Microsoft.AnalysisServices.TraceEventClass.ProgressReportCurrent);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.IntegerData);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.CurrentTime);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectID);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectReference);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.SessionID);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectName);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.DatabaseName);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.StartTime);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.EventSubclass);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.TextData);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ActivityID);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.RequestID);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ProgressTotal);

            /**************************************************************************
                Create the event 2 of 2 to trace [ProgressReportCurrent]
            **************************************************************************/
            TraceEvent traceEvent2 = trace.Events.Add(Microsoft.AnalysisServices.TraceEventClass.ProgressReportEnd);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.IntegerData);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.CurrentTime);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectID);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectReference);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.SessionID);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectName);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.DatabaseName);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.StartTime);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.EndTime);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.EventSubclass);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.TextData);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.Duration);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ActivityID);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.RequestID);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ProgressTotal);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.Success);
            // traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ApplicationName);            

            /**************************************************************************
                Determine the function to handle trace events as the happen
            **************************************************************************/

            trace.OnEvent += new TraceEventHandler(Trace_OnEvent);

            /**************************************************************************
                Save the trace
            **************************************************************************/

            trace.Update(Microsoft.AnalysisServices.UpdateOptions.Default, Microsoft.AnalysisServices.UpdateMode.CreateOrReplace);

            /**************************************************************************
                Start the trace
            **************************************************************************/

            trace.Start();

            /**************************************************************************
                Create an infinite loop that runs every 5 second to check for items
                store in the myQ queue. Post results in batches to the Power BI streaming 
                API endpoint
            **************************************************************************/

            Boolean b = true;
            do
            {

                /**************************************************************************
                    Pause for 5 seconds
                **************************************************************************/

                Thread.Sleep(BatchSleepTime);
                activityFound = false;

                /**************************************************************************
                    Start building JSON string to post data to Power BI
                **************************************************************************/

                jsonString = "{\"rows\": [";

                /**************************************************************************
                    Create a batch of rows from the myQ queue
                **************************************************************************/

                for (int i = 0; i < rowsPerPost && myQ.Count > 0; i++)
                {
                    activityFound = true;
                    row = myQ.Dequeue().ToString();
                    jsonString += $"{row},";
                    rowsProcessed++;
                }
                jsonString += "]}";

                /**************************************************************************
                    If there were items in the queue send them to Power BI now
                **************************************************************************/

                if (activityFound)
                {
                    Task<HttpResponseMessage> postToPowerBI = HttpPostAsync(PowerBIAPI, jsonString);
                }

                /**************************************************************************
                    Update screen to show progress
                **************************************************************************/

                Console.SetCursorPosition(10, 10);
                Console.Write($"Queue Count : {myQ.Count}     ");

                Console.SetCursorPosition(10, 12);
                Console.Write($"Rows Processed : {rowsProcessed}    ");

                Console.SetCursorPosition(10, 14);
                Console.Write($"Current Time : {DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss")}    ");

            } while (b);


            trace.Stop();


        }


        static private void Trace_OnEvent(object sender, TraceEventArgs e)
        {

            if (e.ObjectName != null && e.ObjectReference != null && e.EventSubclass != Microsoft.AnalysisServices.TraceEventSubclass.Backup) // && e.SessionID == _sessionId)
            {
                XmlDocument document = new XmlDocument();
                document.LoadXml(e.ObjectReference);

                XmlNodeList tableNodeList = document.GetElementsByTagName("Table");
                XmlNodeList partitionNodeList = document.GetElementsByTagName("Partition");

                if (
                     e.EventClass == Microsoft.AnalysisServices.TraceEventClass.ProgressReportCurrent

                )
                {
                    string myTableName = tableNodeList[0].InnerText;
                    string myParitionName = partitionNodeList[0].InnerText;

                    string objectName = $"{myTableName}";
                    if (myTableName.ToUpper() != myParitionName.ToUpper() && myParitionName.ToUpper() != "PARTITION") { objectName += $":{myParitionName}"; }

                    myTraceEvent m = new myTraceEvent
                    {
                        CurrentTime = e.CurrentTime,
                        ObjectID = e.ObjectID,
                        ObjectName = objectName,

                        EventSubClass = e.EventSubclass.ToString(),
                        DatabaseName = e.DatabaseName,
                        StartTime = e.StartTime,

                        EventClass = e.EventClass.ToString(),
                        SessionID = e.SessionID,
                        IntegerData = e.IntegerData
                    };
                    string jsonString = $"{JsonConvert.SerializeObject(m)}";

                    myQ.Enqueue(jsonString);
                    //SendToEventHub(jsonString);
                }

                if (
                    e.EventClass == Microsoft.AnalysisServices.TraceEventClass.ProgressReportEnd &&
                    (e.EventSubclass == Microsoft.AnalysisServices.TraceEventSubclass.ExecuteSql ||
                        e.EventSubclass == Microsoft.AnalysisServices.TraceEventSubclass.ReadData
                    )
                )
                {

                    string myTableName = tableNodeList[0].InnerText;
                    string myParitionName = partitionNodeList[0].InnerText;

                    string objectName = $"{myTableName}";
                    if (myTableName.ToUpper() != myParitionName.ToUpper() && myParitionName.ToUpper() != "PARTITION") { objectName += $":{myParitionName}"; }
                  
                    myTraceEvent m = new myTraceEvent
                    {
                        CurrentTime = e.CurrentTime,
                        ObjectID = e.ObjectID,
                        ObjectName = objectName,
                        Duration = (int)e.Duration,
                        EventSubClass = e.EventSubclass.ToString(),
                        DatabaseName = e.DatabaseName,
                        StartTime = e.StartTime,
                        EndTime = e.EndTime,
                        EventClass = e.EventClass.ToString(),
                        SessionID = e.SessionID,
                        IntegerData = e.IntegerData
                    };
                    string jsonString = $"{JsonConvert.SerializeObject(m)}";

                    myQ.Enqueue(jsonString);
                    //SendToEventHub(jsonString);

                }
            }
        }

        static async Task SendToEventHub(String message)
        {
            /**************************************************************************
                Only use if sending data to Azure EventHub
            **************************************************************************/

            EventHubProducerClient producer = new EventHubProducerClient(eventHubConnectionString, eventHubName);

            try
            {
                using var eventBatch = await producer.CreateBatchAsync();

                BinaryData eventBody = new BinaryData(message);
                EventData eventData = new EventData(eventBody);

                if (!eventBatch.TryAdd(eventData))
                {
                    throw new Exception("The first event could not be added.");
                }
                
                await producer.SendAsync(eventBatch);
            }
            finally
            {
                await producer.CloseAsync();
            }
        }


        public class myTraceEvent
        {
            public DateTime CurrentTime { get; set; }
            public string ObjectName { get; set; }
            public string ObjectID { get; set; }
            public int Duration { get; set; }
            public string EventSubClass { get; set; }
            public string DatabaseName { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime EndTime { get; set; }
            public string EventClass { get; set; }
            public long IntegerData { get; set; }
            public string SessionID { get; set; }

        }

        static async Task<HttpResponseMessage> HttpPostAsync(string url, string data)
        {
            // Construct an HttpContent object from StringContent
            HttpContent content = new StringContent(data);
            HttpResponseMessage response = await client.PostAsync(url, content);
            response.EnsureSuccessStatusCode();
            return response;
        }
    }
}