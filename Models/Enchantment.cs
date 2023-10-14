using MessagePack;

namespace Coflnet.Sky.Sniper.Models;
[MessagePackObject]
public struct Enchant
{
    [Key(0)]
    public Core.Enchantment.EnchantmentType Type;
    [Key(1)]
    public byte Lvl;

    public override bool Equals(object obj)
    {
        return obj is Enchant ench
            && ench.Lvl == Lvl
            && ench.Type == Type;
    }

    public override int GetHashCode()
    {
        return (int)Type << 4 + Lvl;
    }

    public override string ToString()
    {
        return base.ToString();
    }
}