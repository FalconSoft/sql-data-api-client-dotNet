using System;
using System.Linq;
using Xunit;

namespace FalconSoft.SqlDataApi.Client.Tests
{
    public class SqlDataApiTests
    {
        private readonly string TestConnectionName = "SQL-Shared";
        public SqlDataApiTests()
        {
            SqlDataApi.SetBaseUrl("https://localhost:44302");
            SqlDataApi.SetAuthentication("12121212-token-212121212");
        }

        private class SimpleObject
        {
            public int OrderId { get; set; }

            public DateTime? OrderDate { get; set; }

            public string Country { get; set; }

            public string City { get; set; }

            public decimal Sales { get; set; }

            public int OrderQuantity { get; set; }
        }

        public class TestObject
        {
            public Product Product { get; set; }
        }

        public class Product
        {
            public Product() { }
            public string ProductCategory { get; set; }
            public string ProductName { get; set; }
            public string ProductContainer { get; set; }
        }

        [Fact]
        public void SimpleTest()
        {
            var items = SqlDataApi
                .Create("SQL-Shared")
                .TableOrView("test1.Sample100")
                .Filter("Country = @country", new { country = "UK" })
                .RunQuery<TestObject>();

            Assert.Equal(7, items.Count);
            Assert.Equal(0, items.Count(r => string.IsNullOrWhiteSpace(r.Product?.ProductName)));
        }

        [Fact]
        public void DatesFilterTest()
        {
            var items = SqlDataApi
                .Create("SQL-Shared")
                .TableOrView("test1.Sample100")
                .Filter("OrderDate = @date", new { date = new DateTime(2017, 2, 25) })
                .RunQuery<TestObject>();

            Assert.Equal(32, items.Count);

            items = SqlDataApi
                .Create("SQL-Shared")
                .TableOrView("test1.Sample100")
                .Filter("OrderDate in @dates", new { dates = new[] { new DateTime(2017, 2, 25), new DateTime(2017, 2, 27) }  })
                .RunQuery<TestObject>();

            Assert.Equal(60, items.Count);
        }

        [Fact]
        public void LimitTest()
        {
            var items = SqlDataApi
                .Create("SQL-Shared")
                .TableOrView("test1.Sample100")
                .Limit(3)
                .RunQuery<TestObject>();

            Assert.Equal(3, items.Count);
        }

        [Fact]
        public void ReadWrite()
        {
            var items = SqlDataApi
                .Create("SQL-Shared")
                .TableOrView("test1.Sample100")
                .Filter("Country = @country", new { country = "UK" })
                .RunQuery<SimpleObject>();

            Assert.Equal(7, items.Count);

            items[0].OrderQuantity = items[0].OrderQuantity + 10;
            var status = SqlDataApi
                .Create(TestConnectionName)
                .TableOrView("test1.Sample100")
                .Save(items);

            Assert.Equal(7, status.Updated);
        }

        [Fact]
        public void TestJoin()
        {
            var items = SqlDataApi
                .Create(TestConnectionName)
                .TableOrView("test1.NorthwindOrders o")
                .InnerJoin("test1.NorthwindEmployees e", "o.EmployeeId = e.EmployeeId")
                .Select("o.OrderId, e.EmployeeId, Concat(e.FirstName, ' ', e.LastName) Employee")
                .Filter("o.ShipCountry = @country", new { country = "UK" })
                .RunQuery<dynamic>();

            Assert.Equal(56, items.Count);
            Assert.Equal(0, items.Count(i => string.IsNullOrWhiteSpace(i.Employee)));
        }

    }
}
