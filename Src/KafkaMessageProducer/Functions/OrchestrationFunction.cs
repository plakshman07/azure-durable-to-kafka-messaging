
using Confluent.Kafka;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.Json;
using System;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask;

using HttpTriggerAttribute = Microsoft.Azure.Functions.Worker.HttpTriggerAttribute;
using ActivityTriggerAttribute = Microsoft.Azure.Functions.Worker.ActivityTriggerAttribute;
using DurableClientAttribute = Microsoft.Azure.Functions.Worker.DurableClientAttribute;
using AuthorizationLevel = Microsoft.Azure.Functions.Worker.AuthorizationLevel;
using OrchestrationTriggerAttribute = Microsoft.Azure.Functions.Worker.OrchestrationTriggerAttribute;

public static class OrchestrationFunction
{
    // Kafka configuration (replace with actual server/port)
    private static readonly string KafkaBroker = "localhost:9092";  // Kafka server address
    private static readonly string KafkaTopic = "durable-function-output"; // Kafka topic to send messages to

    [Function(nameof(OrchestrationFunction))]
    public static async Task<List<string>> RunOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        ILogger logger = context.CreateReplaySafeLogger(nameof(OrchestrationFunction));
        logger.LogInformation("Saying hello.");

        // Call activities in parallel
        var outputs = new List<string>
        {
            await context.CallActivityAsync<string>(nameof(SayHello), "Tokyo"),
            await context.CallActivityAsync<string>(nameof(SayHello), "Seattle"),
            await context.CallActivityAsync<string>(nameof(SayHello), "London")
        };

        // Send results to Kafka
        await SendToKafkaAsync(outputs);

        // Return the outputs to the client or next stage in the workflow
        return outputs;
    }

    // Kafka producer logic
    private static async Task SendToKafkaAsync(List<string> outputs)
    {
        var config = new ProducerConfig { BootstrapServers = KafkaBroker };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        try
        {
            // Serialize the outputs list to JSON
            string message = JsonSerializer.Serialize(outputs);

            // Produce the message to Kafka topic
            var deliveryResult = await producer.ProduceAsync(KafkaTopic, new Message<Null, string> { Value = message });

            Console.WriteLine($"Message sent to Kafka: {deliveryResult.Value}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error sending message to Kafka: {ex.Message}");
        }
    }

    [Function(nameof(SayHello))]
    public static string SayHello([ActivityTrigger] string name, FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("SayHello");
        logger.LogInformation("Saying hello to {name}.", name);
        return $"Hello {name}!";
    }

    [Function("DurableFunctionsOrchestrationCSharp1_HttpStart")]
    public static async Task<HttpResponseData> HttpStart(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req,
        [DurableClient] DurableTaskClient client,
        FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("DurableFunctionsOrchestrationCSharp1_HttpStart");

        // Start the orchestration and get the instance ID
        string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
            nameof(OrchestrationFunction));

        logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

        // Return an HTTP 202 response with an instance management payload
        return await client.CreateCheckStatusResponseAsync(req, instanceId);
    }
}


//using System.Net;
//using System.Text.Json;
//using Microsoft.AspNetCore.Http;
//using Microsoft.AspNetCore.Mvc;
//using Microsoft.Azure.Functions.Worker;
//using Microsoft.Azure.Functions.Worker.Http;
//using Microsoft.DurableTask;
//using Microsoft.DurableTask.Client;
//using Microsoft.Extensions.Logging;

//namespace Company.Function;

//public static class DurableFunctionsOrchestrationCSharp1
//{
//    [Function(nameof(DurableFunctionsOrchestrationCSharp1))]
//    public static async Task<List<string>> RunOrchestrator(
//        [OrchestrationTrigger] TaskOrchestrationContext context)
//    {
//        ILogger logger = context.CreateReplaySafeLogger(nameof(DurableFunctionsOrchestrationCSharp1));
//        logger.LogInformation("Saying hello.");
//        //var outputs = new List<string>();

//        // Replace name and input with values relevant for your Durable Functions Activity
//        //outputs.Add(await context.CallActivityAsync<string>(nameof(SayHello), "Tokyo"));
//        //outputs.Add(await context.CallActivityAsync<string>(nameof(SayHello), "Seattle"));
//        //outputs.Add(await context.CallActivityAsync<string>(nameof(SayHello), "London"));

//        var outputs = new List<string>
//    {
//        await context.CallActivityAsync<string>(nameof(SayHello), "Tokyo"),
//        await context.CallActivityAsync<string>(nameof(SayHello), "Seattle"),
//        await context.CallActivityAsync<string>(nameof(SayHello), "London")
//    };

//        //return ["Hello Tokyo!", "Hello Seattle!", "Hello London!"];
//        return outputs;
//        //return JsonSerializer.Serialize(outputs);
//    }

//    [Function(nameof(SayHello))]
//    public static string SayHello([ActivityTrigger] string name, FunctionContext executionContext)
//    {
//        ILogger logger = executionContext.GetLogger("SayHello");
//        logger.LogInformation("Saying hello to {name}.", name);
//        return $"Hello {name}!";
//    }

//    [Function("DurableFunctionsOrchestrationCSharp1_HttpStart")]
//    public static async Task<HttpResponseData> HttpStart(
//        [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
//        [DurableClient] DurableTaskClient client,
//        FunctionContext executionContext)
//    {
//        ILogger logger = executionContext.GetLogger("DurableFunctionsOrchestrationCSharp1_HttpStart");

//        // Function input comes from the request content.
//        string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
//            nameof(DurableFunctionsOrchestrationCSharp1));

//        logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

//        // Returns an HTTP 202 response with an instance management payload.
//        // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
//        return await client.CreateCheckStatusResponseAsync(req, instanceId);

//    }
//    //[Function("DurableFunctionsOrchestrationCSharp1_HttpStart")]
//    //public static async Task<HttpResponseData> HttpStart(
//    //[HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
//    //[DurableClient] DurableTaskClient client,
//    //FunctionContext executionContext)
//    //{
//    //    ILogger logger = executionContext.GetLogger("DurableFunctionsOrchestrationCSharp1_HttpStart");

//    //    // Start orchestration
//    //    string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(nameof(DurableFunctionsOrchestrationCSharp1));
//    //    logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

//    //    var timeout = TimeSpan.FromSeconds(60);
//    //    var expiry = DateTime.UtcNow + timeout;

//    //    OrchestrationMetadata status = null;

//    //    // Poll until complete or timeout
//    //    while (DateTime.UtcNow < expiry)
//    //    {
//    //        status = await client.GetInstanceAsync(instanceId);

//    //        if (status == null)
//    //        {
//    //            return req.CreateResponse(System.Net.HttpStatusCode.NotFound);
//    //        }

//    //        if (status.RuntimeStatus == OrchestrationRuntimeStatus.Completed ||
//    //            status.RuntimeStatus == OrchestrationRuntimeStatus.Failed ||
//    //            status.RuntimeStatus == OrchestrationRuntimeStatus.Terminated)
//    //        {
//    //            break;
//    //        }

//    //        await Task.Delay(1000);
//    //    }

//    //    var response = req.CreateResponse();

//    //    if (status.RuntimeStatus == OrchestrationRuntimeStatus.Completed)
//    //    {
//    //        // 'output' is a JSON string (e.g. '["Hello Tokyo!","Hello Seattle!","Hello London!"]')
//    //        // Deserialize it into a string array
//    //        var greetings = System.Text.Json.JsonSerializer.Deserialize<string[]>(status.SerializedOutput);

//    //        response.StatusCode = System.Net.HttpStatusCode.OK;
//    //        await response.WriteAsJsonAsync(greetings);
//    //    }
//    //    else
//    //    {
//    //        response.StatusCode = System.Net.HttpStatusCode.Accepted;
//    //        await response.WriteAsJsonAsync(new
//    //        {
//    //            Message = "Orchestration still running or failed",
//    //            status.RuntimeStatus,
//    //            status.InstanceId
//    //        });
//    //    }

//    //    return response;
//    //}

//}