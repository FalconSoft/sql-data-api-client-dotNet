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
            
            public Product Product { get; set; }
            public int OrderQuantity { get; set; }

        }

        public class Product 
        {
            public Product() { }
            public string ProductCategory { get; set; }
            public string ProductName { get; set; }
            public string ProductContainer { get; set; }
        }

        [Fact]
        public void Test1()
        {
            SqlDataApi.SetBaseUrl("https://localhost:44302");
            SqlDataApi.SetAuthentication("121212-token-211212");

            var items = SqlDataApi
                .Create("SQL-Shared")
                .TableOrView("test1.Sample100_ToDelete")
                .Filter("Country = @country", new { country = "UK"})
                .RunQuery<TestObject>();

            // Assert.Equal(7, items.Count);

            items[0].OrderQuantity = items[0].OrderQuantity + 10;
            var status = SqlDataApi
                .Create("SQL-Shared")
                .TableOrView("test1.Sample100_ToDelete")
                .Save(items);

            Assert.NotNull(status);
        }
    }
}
