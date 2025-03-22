using System.Collections.Generic;

namespace Coflnet.Sky.Sniper.Services
{
    public class ModifierLookupKey
    {
        public string ItemTag;
        public KeyValuePair<string, string> Modifier;
        public Dictionary<string, string> RelevantModifiers;

        public override bool Equals(object obj)
        {
            if (obj is not ModifierLookupKey other)
                return false;

            // Compare ItemTag
            if (ItemTag != other.ItemTag)
                return false;

            // Compare Modifier
            if (!Modifier.Equals(other.Modifier))
                return false;

            // Compare RelevantModifiers
            if (RelevantModifiers == null && other.RelevantModifiers == null)
                return true;
            if (RelevantModifiers == null || other.RelevantModifiers == null)
                return false;
            if (RelevantModifiers.Count != other.RelevantModifiers.Count)
                return false;

            // Check if all modifiers in this instance are in the other instance
            foreach (var modifier in RelevantModifiers)
            {
                if (!other.RelevantModifiers.TryGetValue(modifier.Key, out string value) || value != modifier.Value)
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + (ItemTag?.GetHashCode() ?? 0);
            hash = hash * 23 + Modifier.GetHashCode();

            if (RelevantModifiers != null)
            {
                foreach (var modifier in RelevantModifiers)
                {
                    hash = hash * 23 + modifier.Key.GetHashCode();
                    hash = hash * 23 + (modifier.Value?.GetHashCode() ?? 0);
                }
            }

            return hash;
        }
    }
}
