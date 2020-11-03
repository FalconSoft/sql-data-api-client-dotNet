using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FalconSoft.SqlDataApi.Client
{
    internal class LoginUser
    {
        public string Username { get; set; }

        public string Password { get; set; }
    }
    internal class LoggedInUserInfo
    {
        public string Token { get; set; }

        // public UserInfo User { get; set; }
    }


    internal class WebClientEx : WebClient
    {
        /// <summary>
        /// Time in milliseconds
        /// </summary>
        public int Timeout { get; }

        public WebClientEx() : this(5 * 60 * 1000) { }

        public WebClientEx(int timeout)
        {
            this.Timeout = timeout;
        }

        protected override WebRequest GetWebRequest(Uri address)
        {
            var request = base.GetWebRequest(address);
            if (request != null)
            {
                request.Timeout = this.Timeout;
            }
            return request;
        }

        public TResponse Post<TRequest, TResponse>(string url, TRequest requestBody)
        {
            try
            {
                Headers.Add("Content-Type", "application/json");
                var responseJson = UploadString(url, "POST", JsonSerializer.Serialize(requestBody));
                var response = JsonSerializer.Deserialize<TResponse>(responseJson);
                return response;
            }
            catch (WebException ex)
            {
                string msg = "";
                if (ex.Response != null)
                {
                    using (var r = new StreamReader(ex.Response?.GetResponseStream()))
                    {
                        msg = r.ReadToEnd(); // access the reponse message
                    }
                }

                // make sure it has meaningfull message
                throw new ApplicationException($"{ex.Message}: {msg}");
            }
        }
    }

    public partial class SqlDataApi
    {

        internal class SaveDataDto
        {
            public TableDto TableData { get; set; }

            public Dictionary<string, object>[] Items { get; set; }

            public Dictionary<string, object>[] ItemsToDelete { get; set; }
        }

        internal class QueryInfoRequestObject
        {
            public string Select { get; set; }

            public string FilterString { get; set; }

            public Dictionary<string, object> FilterParameters { get; set; }

            public int? Skip { get; set; }

            public int? Top { get; set; }

            public string OrderBy { get; set; }

            public string MainTableAlias { get; set; }

            public IList<TableJoin> TablesJoin { get; set; }
        }

        internal class TableDto
        {
            [JsonConverter(typeof(JsonStringEnumConverter))]
            public enum ConsolidatedDataTypes { String, LargeString, WholeNumber, BigIntNumber, FloatNumber, DateTime, Boolean, Text, Binary }

            [JsonPropertyName("fieldDataTypes")]
            public ConsolidatedDataTypes[] FieldDataTypes { get; set; }

            [JsonPropertyName("fieldNames")]
            public string[] FieldNames { get; set; }

            [JsonPropertyName("rows")]
            public IList<object[]> Rows { get; set; }
        }

        internal class TableJoin
        {
            public string TableName { get; set; }

            public string TableAlias { get; set; }

            [JsonConverter(typeof(JsonStringEnumConverter))]
            public JoinTypes JoinType { get; set; }

            public string JoinCondition { get; set; }
        }

        internal enum JoinTypes { InnerJoin, RightJoin, LeftJoin, FullJoin }

        internal class QueryResult
        {
            [JsonConverter(typeof(JsonStringEnumConverter))]
            public ResultTypes ResultType { get; set; }

            [JsonPropertyName("table")]
            public TableDto Table { get; set; }

            public IList<Dictionary<string, object>> Items { get; set; }

            public IList<ResultFieldInfo> Fields { get; set; }

            public class ResultFieldInfo
            {
                public string FieldName { get; set; }

                [JsonConverter(typeof(JsonStringEnumConverter))]
                public TableDto.ConsolidatedDataTypes DataType { get; set; }
            }

            public enum ResultTypes { Items, Table }
        }
    }



}
