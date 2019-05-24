using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace MessageQueueRecursiveListener
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Amazon.Lambda;
    using Amazon.Lambda.Core;
    using Amazon.Lambda.Model;
    using Amazon.SQS;
    using Amazon.SQS.Model;

    using Newtonsoft.Json.Linq;

    public class Function
    {
        private const string InputQueueUrl = "https://sqs.us-east-2.amazonaws.com/628654266155/PoCQueue";
        private const string OutputQueueUrl = "https://sqs.us-east-2.amazonaws.com/628654266155/YetAnotherQueue";
        private AmazonSQSClient SqSClient => new AmazonSQSClient();  

        public async Task FunctionHandler(JObject input, ILambdaContext context)
        {
            while (true)
            {
                ReceiveMessageRequest request = new ReceiveMessageRequest { QueueUrl = InputQueueUrl, MaxNumberOfMessages = 1 };
                var result = await this.SqSClient.ReceiveMessageAsync(request: request);
                if (result.Messages == null) continue;
                await this.ProcessMessage(message: result.Messages.First(), context: context);
                break;
            }
            


            var invokeLambdaRequest = new InvokeRequest
            {
                FunctionName = "arn:aws:lambda:us-east-2:628654266155:function:MessageQueueRecursiveListener",
                InvocationType = InvocationType.Event,
            };
            
            context.Logger.Log(message: " !! calling now another instance of lambda asynchronously !!  ");
            

            var lambdaClient = new AmazonLambdaClient();
            var lambdaCall = await lambdaClient.InvokeAsync(request: invokeLambdaRequest);
        }


        async Task ProcessMessage(Message message, ILambdaContext context)
        {
           var writeMessageRequest = new SendMessageRequest(OutputQueueUrl, message.Body);
            await this.SqSClient.SendMessageAsync(writeMessageRequest); 
           context.Logger.Log($" Reading of new message -   id =  {message.MessageId} , body = '{message.Body} '.");
            await this.SqSClient.DeleteMessageAsync(InputQueueUrl, message.ReceiptHandle).ContinueWith(result =>
                context.Logger.Log($" Deleting of message id = {message.MessageId}  ."));
        }
    }
}
