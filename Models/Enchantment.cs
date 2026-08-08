using System;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models;
[MessagePackObject]
public struct Enchant : IEquatable<Enchant>
{
    [Key(0)]
    public Core.Enchantment.EnchantmentType Type;
    [Key(1)]
    public byte Lvl;

    // Typed equality so List<Enchant>.Contains / HashSet<Enchant> / EqualityComparer<Enchant>.Default dispatch here
    // instead of the object overload, which BOXES both operands on every comparison — the dominant per-auction Enchant
    // allocation (CheckCombined's Enchants.Contains over every candidate, AuctionKey.Equals, GetReduced). Identical
    // result to the object Equals (Type + Lvl) so it is bit-exact.
    public bool Equals(Enchant other) => other.Lvl == Lvl && other.Type == Type;

    public override bool Equals(object obj) => obj is Enchant ench && Equals(ench);

    public override int GetHashCode()
    {
        return (int)Type << 4 + Lvl;
    }

    public override string ToString()
    {
        return base.ToString();
    }
}