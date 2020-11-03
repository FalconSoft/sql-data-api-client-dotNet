using System;
using System.Collections.Generic;
using System.Text;

namespace FalconSoft.SqlDataApi.Client
{
    [AttributeUsage(AttributeTargets.Class)]
    public class SqlTableAttribute : Attribute
    { 
        public string TableName { get; set; }    
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class SqlFieldAttribute : Attribute
    {
        public string FieldName { get; set; }
        
        public string SqlExpression { get; set; }

        public string Prefix { get; set; }

        public bool Ignore { get; set; }

        public bool SerializeObject { get; set; }

    }

}
