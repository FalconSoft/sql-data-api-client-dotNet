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
            SqlDataApi.SetAuthentication("12121212-token-21212121");
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

        private class TestObject: SimpleObject
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
                .Create(TestConnectionName)
                .TableOrView("test1.Sample100")
                .Filter("Country = @country", new { country = "UK" })
                .RunQuery<TestObject>();

            Assert.Equal(7, items.Count);
            Assert.Empty(items.Where(r => string.IsNullOrWhiteSpace(r.Product?.ProductName)));
            Assert.Empty(items.Where(r => string.IsNullOrWhiteSpace(r.Country)));
        }

        [Fact]
        public void FilterNull()
        {
            var sqlApi = SqlDataApi.Create(TestConnectionName).Table("test1.Sample100");

            var items = sqlApi
                .Filter("OrderPriority is null")
                .RunQuery<TestObject>();

            Assert.Equal(3, items.Count);

            items = sqlApi
                .Filter("OrderPriority is not null")
                .RunQuery<TestObject>();

            Assert.Equal(97, items.Count);
        }

        [Fact]
        public void FilterNullInVariable()
        {
            string priority = null;
            var items = SqlDataApi
                .Create(TestConnectionName)
                .TableOrView("test1.Sample100")
                .Filter("OrderPriority in @priorities", new { priorities = new[] { priority } })
                .RunQuery<TestObject>();

            Assert.Equal(3, items.Count);

            items = SqlDataApi
                .Create(TestConnectionName)
                .TableOrView("test1.Sample100")
                .Filter("OrderPriority not in @priorities", new { priorities = new[] { priority } })
                .RunQuery<TestObject>();

            Assert.Equal(97, items.Count);
        }

        [Fact]
        public void DatesFilterTest()
        {
            var sqlApi = SqlDataApi.Create(TestConnectionName);

            var items = sqlApi
                .TableOrView("test1.Sample100")
                .Filter("OrderDate = @date", new { date = new DateTime(2017, 2, 25) })
                .RunQuery<TestObject>();

            Assert.Equal(32, items.Count);

            items = sqlApi
                .TableOrView("test1.Sample100")
                .Filter("OrderDate in @dates", new { dates = new[] { new DateTime(2017, 2, 25), new DateTime(2017, 2, 27) }  })
                .RunQuery<TestObject>();

            Assert.Equal(60, items.Count);
        }

        [Fact]
        public void LimitTest()
        {
            var items = SqlDataApi
                .Create(TestConnectionName)
                .TableOrView("test1.Sample100")
                .Limit(3)
                .RunQuery<TestObject>();

            Assert.Equal(3, items.Count);
        }

        [Fact]
        public void ReadWrite()
        {
            var items = SqlDataApi
                .Create(TestConnectionName)
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

        [Fact]
        public void GroupByWithJoin()
        {
            var items = SqlDataApi
                .Create(TestConnectionName)
                .TableOrView("test1.NorthwindOrders o")
                .InnerJoin("test1.NorthwindEmployees e", "o.EmployeeId = e.EmployeeId")
                .Select("GroupBy|Concat(e.FirstName, ' ', e.LastName) Employee,COUNT(1) Count, Sum(Freight) Freight")
                .Filter("o.ShipCountry = @country", new { country = "UK" })
                .RunQuery<dynamic>();

            Assert.Equal(9, items.Count);
            Assert.Equal(0, items.Count(i => string.IsNullOrWhiteSpace(i.Employee)));
            Assert.Equal(56, items.Sum(r => r.Count));
        }

        [Fact]
        public void GroupByTest1()
        {
            var items = SqlDataApi
                .Create(TestConnectionName)
                .TableOrView("test1.Sample100")
                .Select("GroupBy|OrderPriority, COUNT(1) Count, SUM(OrderQuantity) Quantities")
                .RunQuery<dynamic>();

            Assert.Equal(6, items.Count);
            Assert.Equal(100, items.Sum(r => r.Count));
            // we have one null group
            Assert.Single(items.Where(r => r.OrderPriority == null));
        }

        [Fact]
        public void GroupByTestWithNullFilter()
        {
            var items = SqlDataApi
                .Create(TestConnectionName)
                .TableOrView("test1.Sample100")
                .Select("GroupBy|OrderPriority, COUNT(1) Count, SUM(OrderQuantity) Quantities")
                .Filter("OrderPriority is not null")
                .RunQuery<dynamic>();

            Assert.Equal(5, items.Count);
            Assert.Equal(97, items.Sum(r => r.Count));
            Assert.Empty(items.Where(r => r.OrderPriority == null));
        }


    }
}
