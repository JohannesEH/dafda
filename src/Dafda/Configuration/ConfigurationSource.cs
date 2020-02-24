namespace Dafda.Configuration
{
    public abstract class ConfigurationSource
    {
        internal static readonly ConfigurationSource Null = new NullConfigurationSource();

        public abstract string GetByKey(string key);

        #region Null Object

        private class NullConfigurationSource : ConfigurationSource
        {
            public override string GetByKey(string key)
            {
                return null;
            }
        }

        #endregion
    }
}