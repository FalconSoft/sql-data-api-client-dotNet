# sql-data-api-client-dotNet
.Net Core client for Sql Data Api. Sql Data Api is RESTApi on top of SQL Server database. Server Side is available as part of Low Code Data Management Platform - Worksheet Systems (https://worksheet.systems)

### Usage

```cs
	// set base url and authentication
	SqlDataApi.SetBaseUrl("https://localhost:44302");
	SqlDataApi.SetAuthentication("*****");
	
	var TestConnectionName = "SQL-Shared"

	// select [List of fields taken from SimpleObject] from test1.Sample100 where Country = 'UK'	
	var items = SqlDataApi
		.Create(TestConnectionName)
		.Table("test1.Sample100")
		.Filter("Country = @country", new { country = "UK" })
		.RunQuery<SimpleObject>();

	items[0].OrderQuantity = items[0].OrderQuantity + 10;
	
	// merge modified collection to the table	
	var status = SqlDataApi
		.Create(TestConnectionName)
		.Table("test1.Sample100")
		.Save(items);	
```

### Tests
More examples can be found in tests
https://github.com/FalconSoft/sql-data-api-client-dotNet/blob/main/src/FalconSoft.SqlDataApi.Client.Tests/SqlData.Client.Tests.cs

### License

A permissive MIT License (c) FalconSoft Ltd.
