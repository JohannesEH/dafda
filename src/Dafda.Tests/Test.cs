using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Tasks;
using Dafda.Consuming;
using Xunit;

namespace Dafda.Tests
{
    public class Test
    {
        private static readonly MethodInfo HandleAsyncMethodInfo = typeof(Test).GetMethod(nameof(ExecuteHandler), BindingFlags.Static | BindingFlags.NonPublic);

        private static readonly ConcurrentDictionary<Type, Delegate> Handlers = new ConcurrentDictionary<Type, Delegate>();

        private delegate Task GenericAsyncHandler<TMessage>(TMessage message, IMessageHandler<TMessage> handler, MessageHandlerContext context)
            where TMessage : class, new();

        [Fact]
        public async Task Can_create_generic_delegate()
        {
            Type messageType = typeof(DummyMessage);

            var del = Handlers.GetOrAdd(messageType, CreateDelegate);

            await Assert.ThrowsAsync<Exception>(() =>
                (Task) del.DynamicInvoke(new DummyMessage(), new DummyMessageHandler(1), new MessageHandlerContext(new Metadata())));
        }

        private static Delegate CreateDelegate(Type messageType)
        {
            var method = HandleAsyncMethodInfo.MakeGenericMethod(messageType);
            var genericType = typeof(GenericAsyncHandler<>).MakeGenericType(messageType);
            return method.CreateDelegate(genericType);
        }

        private static Task ExecuteHandler<TMessage>(TMessage message, IMessageHandler<TMessage> handler, MessageHandlerContext context)
            where TMessage : class, new()
        {
            return handler.Handle(message, context);
        }
    }

    public class DummyMessage
    {
    }

    public class DummyMessageHandler : IMessageHandler<DummyMessage>
    {
        public DummyMessageHandler(int x = 0)
        {
        }

        public async Task Handle(DummyMessage message, MessageHandlerContext context)
        {
            await Task.Delay(5000);
            throw new Exception("HAHA");
        }
    }
}