namespace Dafda.Consuming
{
    /// <summary>
    /// Contains metadata for a message.
    /// </summary>
    public sealed class MessageHandlerContext
    {
        private readonly Metadata _metadata;

        internal MessageHandlerContext(Metadata metadata)
        {
            _metadata = metadata;
        }

        /// <summary>
        /// The message identifier.
        /// </summary>
        public string MessageId => _metadata.MessageId;
        
        /// <summary>
        /// The message type.
        /// </summary>
        public string MessageType => _metadata.Type;

        /// <summary>
        /// Access to message metadata values.
        /// </summary>
        /// <param name="key">A metadata name</param>
        public string this[string key] => _metadata[key];
    }
}