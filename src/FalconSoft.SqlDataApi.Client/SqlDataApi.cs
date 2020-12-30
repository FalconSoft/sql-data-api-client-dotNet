using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FalconSoft.SqlDataApi.Client
{
    public class SaveInfo
    {
        [JsonPropertyName("inserted")]

        public int Inserted { get; set; }

        [JsonPropertyName("updated")]
        public int Updated { get; set; }

        [JsonPropertyName("deleted")]
        public int Deleted { get; set; }
    }

    public interface ISqlDataApi
    {
        ISqlDataApi TableOrView(string tableOrView);

        ISqlDataApi Table(string table);

        ISqlDataApi View(string view);

        ISqlDataApi Select(string fields);
        
        ISqlDataApi Select(IEnumerable<string> fields);

        ISqlDataApi InnerJoin(string tableWithAlias, string joinCondition);
        
        ISqlDataApi LeftJoin(string tableWithAlias, string joinCondition);
        
        ISqlDataApi RightJoin(string tableWithAlias, string joinCondition);

        ISqlDataApi FullJoin(string tableWithAlias, string joinCondition);

        ISqlDataApi Filter(string filterString, object filterParams = null);

        ISqlDataApi OrderBy(string orderByString);

        ISqlDataApi Skip(int skipCount);

        ISqlDataApi Top(int? number = null);

        ISqlDataApi Limit(int? number = null);

        SaveInfo Save<T>(IEnumerable<T> itemsToSave, Dictionary<string, object>[] itemsToDelete = null, int batchNumber = 2000, Action<SaveInfo> progressReportAction = null);

        SaveInfo AppendData<T>(IEnumerable<T> itemsToSave, int batchNumber = 2000, Action<SaveInfo> progressReportAction = null);

        int SaveWithAutoId<T>(T item);

        IList<T> RunQuery<T>();
    }

    public partial class SqlDataApi : ISqlDataApi
    {
        private static Tuple<string, string> _upass;
        private static string _authenticationToken;
        private static string _baseUrl;
        private static string _accessToken;
        private readonly string _connectionName;
        private QueryInfoRequestObject _requestObject = new QueryInfoRequestObject();

        private string _tableOrViewName;
        private bool _isDynamicType;

        public static void SetAuthentication(string userName, string password)
        {
            _upass = Tuple.Create(userName, password);
        }
        public static void SetAuthentication(string accessToken)
        {
            _accessToken = accessToken;
        }

        public static void SetBaseUrl(string baseUrl)
        {
            _baseUrl = baseUrl;
        }

        internal SqlDataApi(string connectionName)
        {
            _connectionName = connectionName;
        }

        public static ISqlDataApi Create(string connectionName)
        {
            return new SqlDataApi(connectionName) as ISqlDataApi;
        }

        public ISqlDataApi Filter(string filterString, object filterParams = null)
        {
            _requestObject.FilterString = filterString;
            _requestObject.FilterParameters = new Dictionary<string, object> { };

            if (filterParams != null)
            {
                foreach (var prop in filterParams.GetType().GetProperties())
                {
                    _requestObject.FilterParameters.Add(prop.Name, prop.GetValue(filterParams));
                }
            }

            return this as ISqlDataApi;
        }

        public ISqlDataApi OrderBy(string orderBy)
        {
            _requestObject.OrderBy = orderBy;
            return this as ISqlDataApi;
        }

        public ISqlDataApi Select(string fields)
        {
            _requestObject.Select = fields;
            return this as ISqlDataApi;
        }
        
        public ISqlDataApi Select(IEnumerable<string> fields)
        {
            return Select(fields != null? string.Join(", ", fields): null);
        }

        public ISqlDataApi Skip(int skipCount)
        {
            _requestObject.Skip = skipCount;
            return this as ISqlDataApi;
        }

        public ISqlDataApi Table(string table)
        {
            return TableOrView(table);
        }

        public ISqlDataApi View(string view)
        {
            return TableOrView(view);
        }

        public ISqlDataApi TableOrView(string tableOrView)
        {
            tableOrView = tableOrView.Trim();
            var spaceIndex = tableOrView.IndexOf(' ');
            if (spaceIndex > 0)
            {
                _tableOrViewName = tableOrView.Substring(0, spaceIndex).Trim();
                _requestObject.MainTableAlias = tableOrView.Substring(spaceIndex).Trim();
            }
            else
            {
                _tableOrViewName = tableOrView;
            }

            return this as ISqlDataApi;
        }

        public ISqlDataApi Top(int? top = null)
        {
            _requestObject.Top = top ?? 0;
            return this as ISqlDataApi;
        }

        public ISqlDataApi Limit(int? top = null)
        {
            _requestObject.Top = top ?? 0;
            return this as ISqlDataApi;
        }

        public ISqlDataApi InnerJoin(string tableWithAlias, string joinCondition)
        {
            AddJoin(JoinTypes.InnerJoin, tableWithAlias, joinCondition);
            return this;
        }

        public ISqlDataApi LeftJoin(string tableWithAlias, string joinCondition)
        {
            AddJoin(JoinTypes.LeftJoin, tableWithAlias, joinCondition);
            return this;
        }

        public ISqlDataApi RightJoin(string tableWithAlias, string joinCondition)
        {
            AddJoin(JoinTypes.RightJoin, tableWithAlias, joinCondition);
            return this;
        }

        public ISqlDataApi FullJoin(string tableWithAlias, string joinCondition)
        {
            AddJoin(JoinTypes.FullJoin, tableWithAlias, joinCondition);
            return this;
        }


        public IList<T> RunQuery<T>()
        {

            if (string.IsNullOrWhiteSpace(_tableOrViewName))
            {
                throw new ApplicationException("Table or View Name must be specified.");
            }

            if (string.IsNullOrWhiteSpace(_connectionName))
            {
                throw new ApplicationException("Connection Name must be specified.");
            }

            if (string.IsNullOrWhiteSpace(_baseUrl))
            {
                throw new ApplicationException("BaseUrl must be specified. please use a static method SqlDataApi.SetBaseUrl(...) or SetBaseUrl inside your pipe");
            }

            var url = $"{_baseUrl.Trim('/')}/sql-data-api/{_connectionName}/query/{_tableOrViewName}";

            _isDynamicType = typeof(object) == typeof(T);

            // if select is not specified, drive it through list of properties
            if (!_isDynamicType && string.IsNullOrWhiteSpace(_requestObject.Select))
            {
                _requestObject.Select = CreateSelectFromType(typeof(T));
            }

            var authToken = GetToken();
            using (var webClient = new WebClientEx())
            {
                url = ApplyAuthentication(url, authToken, webClient);
                var response = webClient.Post<QueryInfoRequestObject, QueryResult>(url, _requestObject);
                // make sure next request object doesn't use any of existing setup
                _requestObject = new QueryInfoRequestObject();
                return _isDynamicType ? TableToListOfDynamics(response.Table).Cast<T>().ToList() : TableToList<T>(response.Table);
            }
        }

        public SaveInfo Save<T>(IEnumerable<T> itemsToSave, Dictionary<string, object>[] itemsToDelete = null, int batchNumber = 2000, Action<SaveInfo> progressReportAction = null)
        {
            if (string.IsNullOrWhiteSpace(_tableOrViewName))
            {
                throw new ApplicationException("Table or View Name must be specified.");
            }

            if (string.IsNullOrWhiteSpace(_connectionName))
            {
                throw new ApplicationException("Connection Name must be specified.");
            }

            if (string.IsNullOrWhiteSpace(_baseUrl))
            {
                throw new ApplicationException("BaseUrl must be specified. please use a static method SqlDataApi.SetBaseUrl(...) or SetBaseUrl inside your pipe");
            }

            var status = new SaveInfo();
            var list = new List<T>();
            foreach (var item in itemsToSave)
            {
                if (list.Count >= batchNumber)
                {
                    var s = Save("save", list);

                    if (progressReportAction != null) 
                    {
                        progressReportAction(s);
                    }

                    status.Inserted += s.Inserted;
                    status.Updated += s.Updated;
                }
                else
                {
                    list.Add(item);
                }
            }

            var last = Save("save", list);

            if (progressReportAction != null)
            {
                progressReportAction(last);
            }

            status.Inserted += last.Inserted;
            status.Updated += last.Updated;

            if (itemsToDelete != null && itemsToDelete.Length > 0)
            {
                Save("save", new object[0], itemsToDelete);
                status.Deleted = itemsToDelete.Length;
            }

            return status;
        }

        public SaveInfo AppendData<T>(IEnumerable<T> itemsToSave, int batchNumber = 2000, Action<SaveInfo> progressReportAction = null)
        {
            if (string.IsNullOrWhiteSpace(_tableOrViewName))
            {
                throw new ApplicationException("Table or View Name must be specified.");
            }

            if (string.IsNullOrWhiteSpace(_connectionName))
            {
                throw new ApplicationException("Connection Name must be specified.");
            }

            if (string.IsNullOrWhiteSpace(_baseUrl))
            {
                throw new ApplicationException("BaseUrl must be specified. please use a static method SqlDataApi.SetBaseUrl(...) or SetBaseUrl inside your pipe");
            }

            var status = new SaveInfo();
            var list = new List<T>();
            foreach (var item in itemsToSave)
            {
                if (list.Count >= batchNumber)
                {
                    var s = Save("append-data", list);

                    if (progressReportAction != null)
                    {
                        progressReportAction(s);
                    }

                    status.Inserted += s.Inserted;
                }
                else
                {
                    list.Add(item);
                }
            }

            var last = Save("append-data", list);

            if (progressReportAction != null)
            {
                progressReportAction(last);
            }

            status.Inserted += last.Inserted;

            return status;
        }

        public int SaveWithAutoId<T>(T item)
        {
            if (string.IsNullOrWhiteSpace(_tableOrViewName))
            {
                throw new ApplicationException("Table or View Name must be specified.");
            }

            if (string.IsNullOrWhiteSpace(_connectionName))
            {
                throw new ApplicationException("Connection Name must be specified.");
            }

            if (string.IsNullOrWhiteSpace(_baseUrl))
            {
                throw new ApplicationException("BaseUrl must be specified. please use a static method SqlDataApi.SetBaseUrl(...) or SetBaseUrl inside your pipe");
            }

            var url = $"{_baseUrl.Trim('/')}/sql-data-api/{_connectionName}/save-with-autoid/{_tableOrViewName}";
            var authToken = GetToken();
            using (var webClient = new WebClientEx())
            {
                url = ApplyAuthentication(url, authToken, webClient);
                var result = webClient.Post<T, int>(url, item);
                return result;
            }
        }

        private SaveInfo Save<T>(string method, IEnumerable<T> itemsToSave, Dictionary<string, object>[] itemsToDelete = null)
        {
            var url = $"{_baseUrl.Trim('/')}/sql-data-api/{_connectionName}/{method}/{_tableOrViewName}";
            var table = ItemsToTable(itemsToSave);
            var saveDataDto = new SaveDataDto
            {
                TableData = table,
                ItemsToDelete = itemsToDelete
            };

            var authToken = GetToken();
            using (var webClient = new WebClientEx())
            {
                url = ApplyAuthentication(url, authToken, webClient);
                var saveStatus = webClient.Post<SaveDataDto, SaveInfo>(url, saveDataDto);
                return saveStatus;
            }
        }


        private static string ApplyAuthentication(string url, string authToken, WebClientEx webClient)
        {
            if (!string.IsNullOrWhiteSpace(authToken))
            {
                webClient.Headers.Add("Authorization", $"Bearer {authToken}");
            }

            if (!string.IsNullOrWhiteSpace(_accessToken))
            {
                url += $"{((url.IndexOf('?') > 0) ? "&" : "?")}$accessToken={_accessToken}";
            }

            return url;
        }

        private string CreateSelectFromType(Type type)
        {
            var fields = new List<string>();

            void populateProperies(Type type, List<string> fields, string prefix = null)
            {
                foreach (var prop in type.GetProperties())
                {
                    var propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

                    var fieldAttribute = prop.GetCustomAttribute<SqlFieldAttribute>();

                    if (fieldAttribute?.Ignore == true)
                    {
                        continue;
                    }

                    if (!propType.IsValueType && propType != typeof(DateTime) && propType != typeof(string))
                    {
                        populateProperies(propType, fields, fieldAttribute?.Prefix);
                    }
                    else if (fieldAttribute == null)
                    {
                        fields.Add(prop.Name);
                    }
                    else if (fieldAttribute.Ignore)
                    {
                        continue;
                    }
                    else if (string.IsNullOrWhiteSpace(fieldAttribute.SqlExpression))
                    {
                        fields.Add(fieldAttribute.FieldName);
                    }
                    else if (!string.IsNullOrWhiteSpace(fieldAttribute.SqlExpression))
                    {
                        // in this case fieldName is an optional field and can be defined in sql expression 
                        fields.Add($"{fieldAttribute.SqlExpression} {fieldAttribute.FieldName}".Trim());
                    }
                }
            }

            populateProperies(type, fields);

            return string.Join(", ", fields);
        }

        private string GetToken()
        {
            if (string.IsNullOrWhiteSpace(_authenticationToken))
            {
                // no authentication means provided
                if (string.IsNullOrWhiteSpace(_authenticationToken) &&
                    (_upass == null || string.IsNullOrWhiteSpace(_upass.Item1) || string.IsNullOrWhiteSpace(_upass.Item2))
                   )
                {
                    return null;
                }

                // authenticate and get token
                using (var webClient = new WebClientEx())
                {
                    var url = $"{_baseUrl}/api/security/authenticate";
                    var response = webClient.Post<LoginUser, LoggedInUserInfo>(url, new LoginUser { Username = _upass.Item1, Password = _upass.Item2 });
                    _authenticationToken = response.Token;
                }
            }

            return _authenticationToken;
        }

        private object JsonElementToObject(JsonElement jsonElement)
        {
            if (jsonElement.ValueKind == JsonValueKind.Null || jsonElement.ValueKind == JsonValueKind.Undefined)
            {
                return null;
            }
            if (jsonElement.ValueKind == JsonValueKind.True)
            {
                return true;
            }
            if (jsonElement.ValueKind == JsonValueKind.False)
            {
                return false;
            }

            if (jsonElement.ValueKind == JsonValueKind.Array)
            {
                return jsonElement.EnumerateArray()
                    .Select(r => JsonElementToObject(r))
                    .ToArray();
            }

            if (jsonElement.ValueKind == JsonValueKind.String)
            {
                var str = jsonElement.GetString();
                return DateTime.TryParse(str, out var dt) ? dt : (object)str;
            }

            if (jsonElement.ValueKind == JsonValueKind.Number)
            {
                return jsonElement.GetDouble();
            }
            return jsonElement.GetRawText();
        }

        /// <summary>
        /// Set Value and change type if needed or if it is possible
        /// </summary>
        private void SetValue<T>(T item, PropertyInfo prop, object value)
        {
            if (prop.PropertyType.Name != value?.GetType()?.Name)
            {
                if (value is JsonElement)
                {
                    value = JsonElementToObject((JsonElement)value);
                }
                var tp = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                value = (value == null) ? null : Convert.ChangeType(value, tp);
            }

            prop.SetValue(item, value);
        }

        private IList<T> TableToList<T>(TableDto table)
        {
            var result = new List<T>();


            var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < table.FieldNames.Length; i++) { dict.Add(table.FieldNames[i], i); }
            var props = typeof(T).GetProperties();

            void assignProperties(object item, object[] row)
            {
                foreach (var prop in item.GetType().GetProperties())
                {
                    var propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    var fieldAttribute = prop.GetCustomAttribute<SqlFieldAttribute>();

                    if (fieldAttribute?.Ignore == true)
                    {
                        continue;
                    }
                    if (!propType.IsValueType && propType != typeof(DateTime) && propType != typeof(string))
                    {
                        // create a complex object
                        var objectValue = Activator.CreateInstance(prop.PropertyType);
                        prop.SetValue(item, objectValue);
                        assignProperties(objectValue, row);
                    }

                    var ind = dict.ContainsKey(prop.Name) ? dict[prop.Name] : -1;

                    if (ind >= 0)
                    {
                        SetValue(item, prop, row[ind]);
                    }
                }


            }

            foreach (var row in table.Rows)
            {
                var item = Activator.CreateInstance<T>();
                assignProperties(item, row);
                result.Add(item);
            }

            return result;
        }

        private IList<dynamic> TableToListOfDynamics(TableDto table)
        {
            var result = new List<dynamic>();


            var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < table.FieldNames.Length; i++) { dict.Add(table.FieldNames[i], i); }


            foreach (var row in table.Rows)
            {
                var item = new ExpandoObject();
                var rowDict = item as IDictionary<string, object>;

                for (int i = 0; i < table.FieldNames.Length; i++)
                {
                    var fieldName = table.FieldNames[i];
                    var dataType = table.FieldDataTypes[i];
                    var value = row[i];
                    rowDict[fieldName] = EnsureValue(value, dataType);
                }


                // assignProperties(item, row);
                result.Add(item);
            }

            return result;
        }

        private object EnsureValue(object value, TableDto.ConsolidatedDataTypes dataType)
        {
            if (value == null)
            {
                return null;
            }

            if (value is JsonElement &&
                (((JsonElement)value).ValueKind == JsonValueKind.Null || ((JsonElement)value).ValueKind == JsonValueKind.Undefined))
            {
                return null;
            }

            if (dataType == TableDto.ConsolidatedDataTypes.DateTime)
            {
                return DateTime.Parse(value.ToString());
            }
            
            if (dataType == TableDto.ConsolidatedDataTypes.FloatNumber)
            {
                return double.Parse(value.ToString());
            }

            if (dataType == TableDto.ConsolidatedDataTypes.WholeNumber)
            {
                return int.Parse(value.ToString());
            }

            if (dataType == TableDto.ConsolidatedDataTypes.Boolean)
            {
                return bool.Parse(value.ToString());
            }

            return value.ToString();
        }

        private TableDto ItemsToTable<T>(IEnumerable<T> items)
        {

            var table = new TableDto { };
            var firstItem = true;
            var isDynamic = false;
            foreach (var item in items)
            {
                if (firstItem)
                {
                    var dataType = item.GetType();
                    isDynamic = item is IDictionary<string, object>;
                    firstItem = false;
                    table.FieldNames = (isDynamic)?
                        (item as IDictionary<string, object>).Keys.ToArray()
                        : dataType.GetProperties().Select(p => p.Name).ToArray();
                    table.Rows = new List<object[]>();
                }

                var row = new object[table.FieldNames.Length];
                for (int i = 0; i < table.FieldNames.Length; i++)
                {
                    var propName = table.FieldNames[i];
                    var value = isDynamic? (item as IDictionary<string, object>)[propName]
                        :  item.GetType().GetProperty(propName).GetValue(item);
                    row[i] = (value is DateTime) ? ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.fff") : value;

                    if (value is DateTime && (DateTime)value < new DateTime(1910, 1, 1))
                    {
                        row[i] = null;
                    }
                }
                table.Rows.Add(row);
            }

            return table;
        }

        private void AddJoin(JoinTypes joinType, string tableWithAlias, string joinCondition)
        {
            if (this._requestObject.TablesJoin == null)
            {
                this._requestObject.TablesJoin = new List<TableJoin>();
            }

            var join = new TableJoin { JoinType = joinType };
            var spaceIndex = tableWithAlias.IndexOf(' ');
            if (spaceIndex < 0)
            {
                throw new ApplicationException("Table name should be specified with alias e.g. dbo.tableName t");
            }

            join.TableName = tableWithAlias.Substring(0, spaceIndex).Trim();
            join.TableAlias = tableWithAlias.Substring(spaceIndex).Trim();
            join.JoinCondition = joinCondition;

            this._requestObject.TablesJoin.Add(join);
        }

    }
}
