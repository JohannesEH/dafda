using Dafda.Producing;

namespace Dafda.Tests.TestDoubles
{
    public class PayloadSerializerStub : IPayloadSerializer
    {
        private readonly string _result;

        public PayloadSerializerStub(string result)
        {
            _result = result;
        }

        public string Serialize(object payload)
        {
            return _result;
        }
    }
}