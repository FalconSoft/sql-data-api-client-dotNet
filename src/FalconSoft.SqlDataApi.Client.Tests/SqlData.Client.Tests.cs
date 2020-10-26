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
            SqlDataApi.SetBaseUrl("https://localhost:44308");
            SqlDataApi.SetAuthentication("pp12-token-21pp");

            var items = SqlDataApi
                .Create("my-laptop-sql")
                .TableOrView("pavlo.SampleSuperstoreSales")
                .Filter("Country = @country", new { country = "UK"})
                .RunQuery<TestObject>();

            // Assert.Equal(7, items.Count);

            items[0].OrderQuantity = items[0].OrderQuantity + 10;
            var status = SqlDataApi
                .Create("my-laptop-sql")
                .TableOrView("pavlo.SampleSuperstoreSales")
                .Save(items);

            Assert.NotNull(status);
        }
    }
}
