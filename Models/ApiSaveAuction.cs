using System.Collections.Generic;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Models;

public class ApiSaveAuction : SaveAuction
{
    public override Dictionary<string, string> FlatenedNBT { get; set; }
}