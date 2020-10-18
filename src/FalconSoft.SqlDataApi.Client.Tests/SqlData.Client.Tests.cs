using System;
using System.Linq;
using Xunit;

namespace FalconSoft.SqlDataApi.Client.Tests
{
    public class SqlDataApiTests
    {
        private class TestObject 
        {
            public int OrderId { get; set; }

            public DateTime? OrderDate { get; set; }
            
            public string Country { get; set; }
            
            public string City { get; set; }
            
            public decimal Sales { get; set; }
            
            public int OrderQuantity { get; set; }
        }

        [Fact]
        public void Test1()
        {
            SqlDataApi.SetBaseUrl("https://worksheets-web-api-5.azurewebsites.net");
            SqlDataApi.SetAuthentication("test1", "1234567");

            var items = SqlDataApi
                .Create("public-data-connect")
                .TableOrView("publicData.Sample100")
                .RunQuery<TestObject>();

            Assert.Equal(100, items.Count);

            items[0].OrderQuantity = items[0].OrderQuantity + 10;
            var status = SqlDataApi
                .Create("public-data-connect")
                .TableOrView("publicData.Sample100")
                .Save(items);

            Assert.NotNull(status);
        }
    }
}
