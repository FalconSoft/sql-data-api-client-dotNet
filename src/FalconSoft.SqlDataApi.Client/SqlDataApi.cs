using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;

namespace FalconSoft.SqlDataApi.Client
{
    public class SaveInfo
    {
        public int Inserted { get; set; }

        public int Updated { get; set; }

        public int Deleted { get; set; }
    }

    public interface ISqlDataApi
    {
        ISqlDataApi TableOrView(string tableOrView);

        ISqlDataApi Table(string table);

        ISqlDataApi View(string view);

        ISqlDataApi Select(string fields);

        ISqlDataApi Filter(string filterString, object filterParams = null);

        ISqlDataApi OrderBy(string orderByString);

        ISqlDataApi Skip(int skipCount);

        ISqlDataApi Top(int? number = null);

        ISqlDataApi Limit(int? number = null);

        SaveInfo Save<T>(IEnumerable<T> itemsToSave, Dictionary<string, object>[] itemsToDelete = null);

        IList<T> RunQuery<T>() where T : new();
    }

    public partial class SqlDataApi : ISqlDataApi
    {
        private static Tuple<string, string> _upass;
        private static string _authenticationToken;
        private static string _baseUrl;
        private static string _accessToken;

        private readonly string _connectionName;

        private readonly QueryInfoRequestObject _requestObject = new QueryInfoRequestObject();

        private string _tableOrViewName;

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

        public ISqlDataApi Skip(int skipCount)
        {
            _requestObject.Skip = skipCount;
            return this as ISqlDataApi;
        }

        public ISqlDataApi Table(string table)
        {
            _tableOrViewName = table;
            return this as ISqlDataApi;
        }

        public ISqlDataApi TableOrView(string tableOrView)
        {
            _tableOrViewName = tableOrView;
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

        public ISqlDataApi View(string view)
        {
            _tableOrViewName = view;
            return this as ISqlDataApi;
        }

        public IList<T> RunQuery<T>() where T : new()
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

            // if select is not specified, drive it through list of properties
            if (string.IsNullOrWhiteSpace(_requestObject.Select))
            {
                _requestObject.Select = string.Join(", ", typeof(T).GetProperties().Select(p => p.Name));
            }

            var authToken = GetToken();
            using (var webClient = new WebClientEx())
            {
                url = ApplyAuthentications(url, authToken, webClient);
                var response = webClient.Post<QueryInfoRequestObject, QueryResult>(url, _requestObject);
                return TableToList<T>(response.Table);
            }
        }

        public SaveInfo Save<T>(IEnumerable<T> itemsToSave, Dictionary<string, object>[] itemsToDelete = null)
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

            var url = $"{_baseUrl.Trim('/')}/sql-data-api/{_connectionName}/save/{_tableOrViewName}";

            var table = ItemsToTable(itemsToSave);
            var saveDataDto = new SaveDataDto
            {
                TableData = table,
                ItemsToDelete = itemsToDelete
            };

            var authToken = GetToken();
            using (var webClient = new WebClientEx())
            {
                url = ApplyAuthentications(url, authToken, webClient);
                var saveStatus = webClient.Post<SaveDataDto, SaveInfo>(url, saveDataDto);
                return saveStatus;
            }
        }

        private static string ApplyAuthentications(string url, string authToken, WebClientEx webClient)
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

        /// <summary>
        /// Set Value and change type if needed or if it is possible
        /// </summary>
        private void SetValue<T>(T item, PropertyInfo prop, object value)
        {
            if (prop.PropertyType.Name != value?.GetType()?.Name)
            {
                var tp = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                value = (value == null) ? null : Convert.ChangeType(value, tp);
            }

            prop.SetValue(item, value);
        }

        private IList<T> TableToList<T>(TableDto table) where T : new()
        {
            var result = new List<T>();


            var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < table.FieldNames.Length; i++) { dict.Add(table.FieldNames[i], i); }
            var props = typeof(T).GetProperties();

            foreach (var row in table.Rows)
            {
                var item = Activator.CreateInstance<T>();

                foreach (var prop in props)
                {
                    var ind = dict.ContainsKey(prop.Name) ? dict[prop.Name] : -1;

                    if (ind >= 0)
                    {
                        SetValue<T>(item, prop, row[ind]);
                    }
                }

                result.Add(item);
            }

            return result;
        }

        private TableDto ItemsToTable<T>(IEnumerable<T> items)
        {
            var table = new TableDto { };
            var firstItem = true;
            foreach (var item in items)
            {
                if (firstItem)
                {
                    firstItem = false;
                    table.FieldNames = item.GetType().GetProperties().Select(p => p.Name).ToArray();
                    table.Rows = new List<object[]>();
                }

                var row = new object[table.FieldNames.Length];
                for (int i = 0; i < table.FieldNames.Length; i++)
                {
                    var propName = table.FieldNames[i];
                    var value = item.GetType().GetProperty(propName).GetValue(item);
                    row[i] = (value is DateTime) ? ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss") : value;
                }
                table.Rows.Add(row);
            }

            return table;
        }

    }
}
