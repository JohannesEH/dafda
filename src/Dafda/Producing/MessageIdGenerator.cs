using System;

namespace Dafda.Producing
{
    public abstract class MessageIdGenerator
    {
        internal static readonly MessageIdGenerator Default = new DefaultMessageIdGenerator();

        public abstract string NextMessageId();

        private class DefaultMessageIdGenerator : MessageIdGenerator
        {
            public override string NextMessageId()
            {
                return Guid.NewGuid().ToString();
            }
        }
    }
}