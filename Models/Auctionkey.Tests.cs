using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Models
{
    public class AuctionkeyTests
    {
        [Test]
        public void Test1()
        {
            var key = new AuctionKey();
            // by default reforge and tier match
            Assert.AreEqual(2, key.Similarity(key));
        }
    }

}