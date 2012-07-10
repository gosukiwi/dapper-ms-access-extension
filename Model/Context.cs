using System;
using System.Linq;
using System.Text;
using System.Data;
using System.Reflection;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using Dapper;

namespace Model
{
    /// <summary>
    /// Extension class for Dapper with basic Insert, Update and Delete that works on OleDB
    /// @Author: Federico Ramírez
    /// @Copyright: Paradigma <http://paradigma.com.ar>
    /// </summary>
    public class Context : IDisposable
    {
        #region Properties
        private bool TransactionActive = true;
        private IDbConnection _conn;
        public IDbConnection Connection
        {
            get
            {
                return _conn;
            }
        }
        public ConnectionType Type { get; set; }
        public enum ConnectionType
        {
            OleDB, SqlServer
        }
        public string ConnectionString { private set; get; }

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName = new ConcurrentDictionary<RuntimeTypeHandle, string>();
        private IDbTransaction transaction;

        public enum CascadeStyle { Collection, Single, All, None }
        #endregion

        public Context(string connectionString, ConnectionType type = ConnectionType.OleDB, bool useWebConfig = true)
        {
            this.ConnectionString = connectionString;
            this.Type = type;

            switch (type)
            {
                case ConnectionType.OleDB:
                    _conn = new System.Data.OleDb.OleDbConnection(useWebConfig
                        ? ReadConnectionString(connectionString)
                        : connectionString);
                    break;
                default:
                    _conn = new System.Data.SqlClient.SqlConnection(useWebConfig
                        ? ReadConnectionString(connectionString)
                        : connectionString);
                    break;
            }

            _conn.Open();
            transaction = _conn.BeginTransaction();
        }

        private string ReadConnectionString(string connectionString)
        {
            string conn = null;
            try
            {
                conn = System.Configuration.ConfigurationManager.ConnectionStrings[connectionString].ConnectionString;
            }
            catch
            {
                throw new InvalidOperationException("Could not find connection string: " + connectionString);
            }

            if (string.IsNullOrEmpty(conn))
            {
                throw new InvalidOperationException("Could not find connection string: " + connectionString);
            }

            return conn;
        }

        public void Dispose()
        {
            if (_conn != null)
            {
                if (_conn.State == ConnectionState.Open)
                {
                    transaction.Dispose();
                    _conn.Close();
                }
                _conn.Dispose();
            }
        }

        /// <summary>
        /// Updates an object on the database when the SubmitChanges method is called
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object to update</param>
        /// <param name="cascade">Whether to update child/parent objects too</param>
        public void UpdateOnSubmit<T>(T obj, CascadeStyle cascade = CascadeStyle.None) where T : class
        {
            if (obj == null) return;
            if (!TransactionActive)
            {
                transaction = _conn.BeginTransaction();
                TransactionActive = true;
            }

            Type type = typeof(T);
            string tableName = GetTableName(type);

            List<string> fields = new List<string>();
            List<string> values = new List<string>();
            List<object> data = new List<object>();

            string pk = "id";
            object pkValue = null;

            foreach (PropertyInfo pi in type.GetProperties())
            {
                bool ispk = (from a in pi.GetCustomAttributes(false) where a.GetType().Name.Equals("KeyAttribute") select a).Any();
                if (ispk || pi.Name.Equals(pk, StringComparison.OrdinalIgnoreCase))
                {
                    pk = pi.Name;
                    pkValue = pi.GetValue(obj, null);
                }
                else
                {
                    if ((cascade == CascadeStyle.All || cascade == CascadeStyle.Collection)
                        && IsCollection(pi.PropertyType) && pi.GetValue(obj, null) != null)
                    {
                        foreach (var child in pi.GetValue(obj, null) as ICollection)
                        {
                            MethodInfo mi = typeof(Context).GetMethod("UpdateOnSubmit");
                            MethodInfo genericMethod = mi.MakeGenericMethod(new Type[] { child.GetType() });
                            genericMethod.Invoke(this, new Object[] { child, cascade });
                        }
                    }
                    else if ((cascade == CascadeStyle.All || cascade == CascadeStyle.Single)
                        && !IsPrimitive(pi.PropertyType))
                    {   // If it's not a primitive I'll try to insert it as a child object
                        MethodInfo mi = typeof(Context).GetMethod("UpdateOnSubmit");
                        MethodInfo genericMethod = mi.MakeGenericMethod(new Type[] { pi.PropertyType });
                        genericMethod.Invoke(this, new Object[] { pi.GetValue(obj, null), cascade });
                    }
                    else
                    {
                        fields.Add("[" + pi.Name + "] = @" + pi.Name);
                        values.Add("@" + pi.Name);
                        data.Add(pi.GetValue(obj, null));
                    }
                }
            }

            if (pkValue == null)
            {
                throw new Exception("Could not find primary key");
            }

            IDbCommand com = _conn.CreateCommand();
            com.Transaction = transaction;
            com.CommandText = string.Format("UPDATE {0} SET {1} WHERE {2} = {3}", tableName, string.Join(", ", fields), pk, pkValue);
            for (int i = 0; i < values.Count; i++)
            {
                IDbDataParameter p = com.CreateParameter();
                p.ParameterName = values[i];
                p.Value = data[i];
                com.Parameters.Add(p);
            }

            try
            {
                com.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Deletes the object from the database when the method SubmitChanges is called
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <param name="cascade">Whether the child/parent objects will also be deleted
        /// Note: It's recommended to leave that kind of logic to the database engine</param>
        public void DeleteOnSubmit<T>(T obj, CascadeStyle cascade = CascadeStyle.None) where T : class
        {
            if (!TransactionActive)
            {
                transaction = _conn.BeginTransaction();
                TransactionActive = true;
            }

            Type type = typeof(T);
            string tableName = GetTableName(type);

            string pk = "id";
            object pkValue = null;

            foreach (PropertyInfo pi in type.GetProperties())
            {
                bool ispk = (from a in pi.GetCustomAttributes(false) where a.GetType().Name.Equals("KeyAttribute") select a).Any();
                if (ispk || pi.Name.Equals(pk, StringComparison.OrdinalIgnoreCase))
                {
                    pk = pi.Name;
                    pkValue = pi.GetValue(obj, null);
                }
                else if (cascade != CascadeStyle.None)
                {
                    if (pi.GetValue(obj, null) == null) continue;

                    if ((cascade == CascadeStyle.Collection || cascade == CascadeStyle.All)
                        && IsCollection(pi.PropertyType))
                    {
                        foreach (var child in pi.GetValue(obj, null) as ICollection)
                        {
                            MethodInfo mi = typeof(Context).GetMethod("DeleteOnSubmit");
                            MethodInfo genericMethod = mi.MakeGenericMethod(new Type[] { child.GetType() });
                            genericMethod.Invoke(this, new Object[] { child, cascade });
                        }
                    }
                    else if ((cascade == CascadeStyle.Single || cascade == CascadeStyle.All)
                        && !IsPrimitive(pi.PropertyType))
                    {
                        MethodInfo mi = typeof(Context).GetMethod("DeleteOnSubmit");
                        MethodInfo genericMethod = mi.MakeGenericMethod(new Type[] { pi.PropertyType });
                        genericMethod.Invoke(this, new Object[] { pi.GetValue(obj, null), cascade });
                    }
                }
            }

            IDbCommand com = _conn.CreateCommand();
            com.Transaction = transaction;
            com.CommandText = "DELETE FROM " + tableName + " WHERE [" + pk + "] = " + pkValue;
            com.ExecuteNonQuery();
        }

        // TODO: Use MSIL or something better than default reflection...
        /// <summary>
        /// Adds an object to the insertion queue
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns>The id of the inserted object</returns>
        public int InsertOnSubmit<T>(T obj) where T : class
        {
            if (obj == null) return 0;
            if (!TransactionActive)
            {
                transaction = _conn.BeginTransaction();
                TransactionActive = true;
            }

            Type type = typeof(T);
            string tableName = GetTableName(type);

            List<string> names = new List<string>();
            List<string> values = new List<string>();
            List<object> data = new List<object>();

            string pk = "id";
            PropertyInfo piPk = null;

            foreach (PropertyInfo pi in type.GetProperties())
            {
                bool ispk = (from a in pi.GetCustomAttributes(false) where a.GetType().Name.Equals("KeyAttribute") select a).Any();
                if (ispk || pi.Name.Equals(pk, StringComparison.OrdinalIgnoreCase))
                {
                    pk = pi.Name;
                    piPk = pi;
                }
                else
                {
                    if (IsCollection(pi.PropertyType) && pi.GetValue(obj, null) != null)
                    {
                        foreach (var child in pi.GetValue(obj, null) as ICollection)
                        {
                            MethodInfo mi = typeof(Context).GetMethod("InsertOnSubmit");
                            MethodInfo genericMethod = mi.MakeGenericMethod(new Type[] { child.GetType() });
                            genericMethod.Invoke(this, new Object[] { child });
                        }
                    }
                    else if (!IsPrimitive(pi.PropertyType))
                    {   // If it's not a primitive I'll try to insert it as a child object
                        MethodInfo mi = typeof(Context).GetMethod("InsertOnSubmit");
                        MethodInfo genericMethod = mi.MakeGenericMethod(new Type[] { pi.PropertyType });
                        int id = (int)genericMethod.Invoke(this, new Object[] { pi.GetValue(obj, null) });
                        if (id > 0)
                        {   // If the value was not null
                            // Check the foreign key
                            var fk = (from a in pi.GetCustomAttributes(false) where a.GetType().Name.Equals("ForeignKeyAttribute") select a).FirstOrDefault() as dynamic;
                            string fkString = pi.Name + "Id";
                            if (fk != null)
                                fkString = fk.Name;

                            names.Add("[" + fkString + "]");
                            values.Add("@" + fkString);
                            data.Add(id);
                        }
                    }
                    else
                    {
                        names.Add("[" + pi.Name + "]");
                        values.Add("@" + pi.Name);
                        data.Add(pi.GetValue(obj, null));
                    }
                }
            }

            IDbCommand com = _conn.CreateCommand();
            com.Transaction = transaction;
            com.CommandText = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", tableName, string.Join(", ", names), string.Join(", ", values));
            for (int i = 0; i < values.Count; i++)
            {
                IDbDataParameter p = com.CreateParameter();
                p.ParameterName = values[i];
                p.Value = data[i];
                com.Parameters.Add(p);
            }

            try
            {
                com.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }

            switch (this.Type)
            {
                case ConnectionType.OleDB:
                    com.CommandText = string.Format("SELECT TOP 1 {0} FROM {1} ORDER BY {0} DESC", pk, tableName);
                    break;
                case ConnectionType.SqlServer:
                    com.CommandText = string.Format("SELECT cast(SCOPE_IDENTITY() as int) AS '{0}'", pk);
                    break;
            }

            int result = -1;
            if (!int.TryParse(com.ExecuteScalar().ToString(), out result))
            {
                throw new Exception("Could not get last primary key");
            }

            piPk.SetValue(obj, result, null);
            return result;
        }

        /// <summary>
        /// Finds an object in the database
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="comparator"></param>
        /// <returns></returns>
        public IEnumerable<T> Find<T>(Func<T, bool> comparator) where T : class
        {
            Type type = typeof(T);
            string tableName = GetTableName(type);
            return this.Connection.Query<T>("SELECT * FROM [" + tableName + "]").Where(comparator);
        }

        public T Find<T>(string pkValue, string pkField = null) where T : class
        {
            Type type = typeof(T);
            string tableName = GetTableName(type);

            // get the primary key field
            string pk;
            if (!string.IsNullOrEmpty(pkField))
            {
                pk = pkField;
            }
            else
            {
                pk = "id";
                foreach (PropertyInfo pi in type.GetProperties())
                {
                    bool ispk = (from a in pi.GetCustomAttributes(false) where a.GetType().Name.Equals("KeyAttribute") select a).Any();
                    if (ispk || pi.Name.Equals(pk, StringComparison.OrdinalIgnoreCase))
                    {
                        pk = pi.Name;
                        break;
                    }
                }
            }

            return this.Connection.Query<T>("SELECT * FROM [" + tableName + "] WHERE [" + pk + "] = '" + pkValue + "'").FirstOrDefault();
        }

        public bool SubmitChanges()
        {
            try
            {
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }

            TransactionActive = false;
            transaction.Dispose();

            return true;
        }

        private bool IsCollection(System.Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ICollection<>);
        }

        private bool IsPrimitive(System.Type type)
        {
            return type.IsPrimitive || type == typeof(Char) || type == typeof(String) || type == typeof(Decimal) || type == typeof(DateTime);
        }

        private string GetTableName(Type type)
        {
            string name;
            if (!TypeTableName.TryGetValue(type.TypeHandle, out name))
            {
                name = type.Name + "s";
                if (type.IsInterface && name.StartsWith("I"))
                    name = name.Substring(1);

                //NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableattr = type.GetCustomAttributes(false).Where(attr => attr.GetType().Name == "TableAttribute").SingleOrDefault() as
                    dynamic;
                if (tableattr != null)
                    name = tableattr.Name;
                TypeTableName[type.TypeHandle] = name;
            }
            return name;
        }
    }

    #region Attributes for classes and properties
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }
        public string Name { get; private set; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ForeignKeyAttribute : Attribute
    {
        public ForeignKeyAttribute(string fkName)
        {
            Name = fkName;
        }
        public string Name { get; private set; }
    }
    #endregion
}
