using System;
using System.Data;
using System.Configuration;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Web.UI.HtmlControls;
using System.Data.SqlClient;

namespace CommContracts
{
    public class DBUtility
    {
        /// <summary>     
        /// Defines that string is pre-configured in web.config file or entered manually in the code
        /// </summary>        
        public enum ConnectionStringType
        {
            Configured = 2,
            Manual = 1
        }
        #region Private fields..

        private string _strConnection;
        private SqlCommand _sqlCommand;
        private SqlConnection _sqlConnection;
        private SqlDataAdapter _sqlDataAdapter;
        private DataSet _dataSet;

        #endregion
        #region Properties..
        /// <summary>
        /// Gets or Sets the sql database  connection string
        /// </summary>

        public string ConnectionString
        {
            get { return _strConnection; }
            set { _strConnection = value; }
        }
        /// <summary>
        /// Gets or Sets the sql  sqlcommand
        /// </summary>
        private SqlCommand Command
        {
            get { return _sqlCommand; }
            set { _sqlCommand = value; }
        }

        /// <summary>
        /// Gets or Sets the sql  sqlconncetion object
        /// </summary>
        private SqlConnection Connection
        {
            get { return _sqlConnection; }
            set { _sqlConnection = value; }
        }
        /// <summary>
        /// Gets or Sets the sql  SqlDataAdapter object
        /// </summary>
        private SqlDataAdapter DataAdapter
        {
            get { return _sqlDataAdapter; }
            set { _sqlDataAdapter = value; }
        }

        /// <summary>
        /// Gets or Sets the dataset object
        /// </summary>
        private DataSet Database
        {
            get { return _dataSet; }
            set { _dataSet = value; }
        }

        #endregion
        #region Constructor..
        /// <summary>
        /// Initializes the DBUtility class with sqlconnection initialization.  
        /// </summary>
        /// <param name="strConnectionKey"  >Connection string key value specified in web.config or connection string itself  </param>        
        /// <param name="type"></param>        
        public DBUtility(string strConnectionKey, ConnectionStringType type)
        {
            if ((strConnectionKey.Equals(string.Empty) && (type != ConnectionStringType.Configured)) && (type != ConnectionStringType.Configured))
            {
                throw new ArgumentException();
            }
            if (ConnectionStringType.Configured == type)
            {
                try
                {
                    Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[strConnectionKey].ConnectionString);
                }
                catch (Exception)
                {
                    try
                    {
                        Connection = new SqlConnection(System.Configuration.ConfigurationManager.AppSettings[strConnectionKey]);
                        //Connection = new SqlConnection(ConfigurationSettings.AppSettings[strConnectionKey]);
                    }
                    catch (Exception exception)
                    {
                        throw exception;
                    }
                }
            }
            else if ((ConnectionStringType.Manual == type) && (this.ConnectionString != string.Empty))
            {
                Connection = new SqlConnection(strConnectionKey);
            }
        }

        #endregion
        #region Public Methods..
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand" value="SqlCommand object" ></param>       
        /// <param name="parameterNames"></param>
        /// <param name="dataTypes"></param>
        /// <param name="values"></param>
        public static void AddParameters(SqlCommand sqlCommand, string[] parameterNames, SqlDbType[] dataTypes, object[] values)
        {
            int index = 0;
            foreach (SqlDbType type in dataTypes)
            {
                IConvertible convertible = (IConvertible)values[index];
                sqlCommand.Parameters.Add(parameterNames[index].ToString(), type);
                sqlCommand.Parameters[parameterNames[index]].Value = convertible;
                index++;
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlDataAdapter"></param>
        /// <param name="parameterNames"></param>
        /// <param name="dataTypes"></param>
        /// <param name="values"></param>
        public static void AddParameters(SqlDataAdapter sqlDataAdapter, string[] parameterNames, SqlDbType[] dataTypes, object[] values)
        {
            int index = 0;
            foreach (SqlDbType type in dataTypes)
            {
                IConvertible convertible = (IConvertible)values[index];
                sqlDataAdapter.SelectCommand.Parameters.Add(parameterNames[index].ToString(), type);
                sqlDataAdapter.SelectCommand.Parameters[parameterNames[index]].Value = convertible;
                index++;
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlString"></param>
        /// <returns>Dataset</returns>
        public DataTable ExecuteDataTable(string sqlString)
        {
            DataTable set;
            try
            {
                this.DataAdapter = InitalizeDataAdapterObject(this.DataAdapter, sqlString);
                set = this.FillDataToDataTable(this.DataAdapter);
            }
            catch (Exception exception)
            {
                throw exception;
            }
            finally
            {
                if (Connection.State != ConnectionState.Closed)
                    Connection.Close();
            }

            return set;
        }/// 
        public DataTable ExecuteDataTable(string procedureName, params SqlParameter[] parameters)
        {
            DataTable set;

            try
            {
                this.DataAdapter = InitalizeDataAdapterObject(this.DataAdapter, procedureName);
                this.DataAdapter.SelectCommand.Parameters.AddRange(parameters);

                set = this.FillDataToDataTable(this.DataAdapter);
            }
            catch (Exception exception)
            {
                throw exception;
            }
            finally
            {
                if (Connection.State != ConnectionState.Closed)
                    Connection.Close();
            }
            return set;
        }

        public DataSet ExecuteDataSet(string sqlString)
        {
            DataSet set;
            try
            {
                this.DataAdapter = InitalizeDataAdapterObject(this.DataAdapter, sqlString);
                set = this.FillDataToDataSet(this.DataAdapter);
            }
            catch (Exception exception)
            {
                throw exception;
            }
            finally
            {
                if (Connection.State != ConnectionState.Closed)
                    Connection.Close();
            }

            return set;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="procedureName"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(string procedureName, params SqlParameter[] parameters)
        {
            DataSet set;
            try
            {
                this.DataAdapter = InitalizeDataAdapterObject(this.DataAdapter, procedureName);
                this.DataAdapter.SelectCommand.Parameters.AddRange(parameters);
                set = this.FillDataToDataSet(this.DataAdapter);
            }
            catch (Exception exception)
            {
                throw exception;
            }
            finally
            {
                if (Connection.State != ConnectionState.Closed)
                    Connection.Close();
            }
            return set;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterNames"></param>
        /// <param name="dataTypes"></param>
        /// <param name="values"></param>
        /// <param name="strSql"></param>
        /// <returns></returns>
        /// 




        public DataSet ExecuteDataSet(string parameterNames, SqlDbType dataTypes, object values, string strSql)
        {
            DataSet set;
            try
            {
                this.DataAdapter = InitalizeDataAdapterObject(this.DataAdapter, strSql);
                this.DataAdapter.SelectCommand.Parameters.Add(parameterNames, dataTypes);
                this.DataAdapter.SelectCommand.Parameters[parameterNames].Value = values;
                set = this.FillDataToDataSet(this.DataAdapter);
            }
            catch (Exception exception)
            {

                throw exception;
            }
            finally
            {
                if (Connection.State != ConnectionState.Closed)
                    Connection.Close();
            }

            return set;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterNames"></param>
        /// <param name="dataTypes"></param>
        /// <param name="values"></param>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(string[] parameterNames, SqlDbType[] dataTypes, object[] values, string strSql)
        {
            DataSet set;
            try
            {
                this.DataAdapter = InitalizeDataAdapterObject(this.DataAdapter, strSql);
                AddParameters(this.DataAdapter, parameterNames, dataTypes, values);
                set = this.FillDataToDataSet(this.DataAdapter);
            }
            catch (Exception exception)
            {
                throw exception;
            }
            finally
            {
                Connection.Close();
            }
            return set;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(SqlCommand sqlCommand)
        {
            int num = -1;
            try
            {
                if (Connection.State == ConnectionState.Closed)
                {
                    Connection.Open();
                }
                num = sqlCommand.ExecuteNonQuery();
            }
            catch (Exception)
            { }
            finally
            {
                if (Connection.State == ConnectionState.Open)
                {
                    Connection.Close();
                }
            }
            return num;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlDataAdapter"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(SqlDataAdapter sqlDataAdapter)
        {
            int num = -1;
            try
            {
                if (Connection.State == ConnectionState.Closed)
                {
                    Connection.Open();
                }
                num = sqlDataAdapter.SelectCommand.ExecuteNonQuery();
            }
            finally
            {
                if (Connection.State == ConnectionState.Open)
                {
                    Connection.Close();
                }
            }
            return num;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="procedureName"></param>
        /// <param name="parameters"></param>
        /// <returns ></returns>
        public int ExecuteNonQuery(string procedureName, params SqlParameter[] parameters)
        {
            try
            {
                this.Command = InitalizeCommandObject(this.Command, procedureName);
                this.Command.Parameters.AddRange(parameters);
                return this.ExecuteNonQuery(this.Command);
            }
            catch (Exception)
            {
                return 0;
            }
            finally
            {
                if (Connection.State != ConnectionState.Closed)
                    Connection.Close();
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterNames"></param>
        /// <param name="dataTypes"></param>
        /// <param name="values"></param>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public bool ExecuteNonQuery(string[] parameterNames, SqlDbType[] dataTypes, object[] values, string strSql)
        {
            bool flag;
            try
            {
                this.Command = InitalizeCommandObject(this.Command, strSql);
                AddParameters(this.Command, parameterNames, dataTypes, values);
                this.ExecuteNonQuery(this.Command);
                flag = true;
            }
            catch (SqlException exception)
            {
                throw exception;
            }
            catch (Exception exception2)
            {
                throw exception2;
            }
            finally
            {
                Connection.Close();
            }
            return flag;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        public SqlDataReader ExecuteReader(SqlCommand sqlCommand)
        {
            SqlDataReader reader = null;
            try
            {
                if (Connection.State == ConnectionState.Closed)
                {
                    Connection.Open();
                }
                reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
            }
            finally
            {
                if (Connection.State == ConnectionState.Open)
                    Connection.Close();
            }
            return reader;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="procedureName"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public SqlDataReader ExecuteReader(string procedureName, params SqlParameter[] parameters)
        {
            SqlDataReader reader = null;
            try
            {
                if (Connection.State == ConnectionState.Closed)
                {
                    Connection.Open();
                }
                this.Command = InitalizeCommandObject(this.Command, procedureName);
                this.Command.Parameters.AddRange(parameters);
                reader = this.ExecuteReader(this.Command);
            }
            catch
            {
            }
            finally
            {
                //if (Connection.State == ConnectionState.Open)
                //    Connection.Close();
            }
            return reader;

        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        public object ExecuteScalar(SqlCommand sqlCommand)
        {
            object obj2 = null;
            try
            {
                if (Connection.State == ConnectionState.Closed)
                {
                    Connection.Open();
                }
                obj2 = sqlCommand.ExecuteScalar();
            }
            finally
            {
                if (Connection.State == ConnectionState.Open)
                {
                    Connection.Close();
                }
            }
            return obj2;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlString"></param>
        /// <returns></returns>
        public object ExecuteScalar(string sqlString)
        {
            object obj2 = null;
            try
            {
                if (Connection.State == ConnectionState.Closed)
                {
                    Connection.Open();
                }
                this.Command = InitalizeCommandObject(this.Command, sqlString);
                obj2 = this.Command.ExecuteScalar();
            }
            finally
            {
                if (Connection.State == ConnectionState.Open)
                {
                    Connection.Close();
                }
            }
            return obj2;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="procedureName"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public object ExecuteScalar(string procedureName, params SqlParameter[] parameters)
        {
            try
            {
                this.Command = InitalizeCommandObject(this.Command, procedureName);
                this.Command.Parameters.AddRange(parameters);
                return this.ExecuteScalar(this.Command);
            }
            finally
            {
                if (Connection.State != ConnectionState.Closed)
                    Connection.Close();
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterNames"></param>
        /// <param name="dataTypes"></param>
        /// <param name="values"></param>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public object ExecuteScalar(string[] parameterNames, SqlDbType[] dataTypes, object[] values, string strSql)
        {
            object obj2;
            try
            {
                this.Command = InitalizeCommandObject(this.Command, strSql);
                AddParameters(this.Command, parameterNames, dataTypes, values);
                obj2 = this.ExecuteScalar(this.Command);
            }
            catch (Exception exception)
            {
                throw exception;
            }
            finally
            {
                if (Connection.State != ConnectionState.Closed)
                    Connection.Close();
            }
            return obj2;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlDataAdapter"></param>
        /// <returns></returns>
        public DataSet FillDataToDataSet(SqlDataAdapter sqlDataAdapter)
        {
            this.Database = new DataSet();
            sqlDataAdapter.Fill(this.Database);
            return this.Database;
        }
        public DataTable FillDataToDataTable(SqlDataAdapter sqlDataAdapter)
        {
            this.Database = new DataSet();
            sqlDataAdapter.Fill(this.Database);
            return this.Database.Tables[0];
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramName"></param>
        /// <param name="paramValue"></param>
        /// <returns></returns>
        public static SqlParameter GetInOutParameter(string paramName, object paramValue)
        {
            SqlParameter parameter = new SqlParameter(paramName, paramValue);
            parameter.Direction = ParameterDirection.InputOutput;

            return parameter;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramName"></param>
        /// <param name="paramValue"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public static SqlParameter GetInOutParameter(string paramName, object paramValue, int size)
        {
            SqlParameter parameter = new SqlParameter(paramName, paramValue);
            parameter.Size = size;
            parameter.Direction = ParameterDirection.InputOutput;
            return parameter;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramName"></param>
        /// <param name="paramValue"></param>
        /// <returns></returns>
        public static SqlParameter GetInParameter(string paramName, object paramValue)
        {
            return new SqlParameter(paramName, paramValue);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramName"></param>
        /// <param name="paramValue"></param>
        /// <returns></returns>
        public static SqlParameter GetOutParameter(string paramName, object paramValue)
        {
            SqlParameter parameter = new SqlParameter(paramName, paramValue);
            parameter.Direction = ParameterDirection.Output;
            return parameter;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramName"></param>
        /// <param name="paramValue"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public static SqlParameter GetOutParameter(string paramName, object paramValue, int size)
        {
            SqlParameter parameter = new SqlParameter(paramName, paramValue);
            parameter.Size = size;
            parameter.Direction = ParameterDirection.InputOutput;
            return parameter;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="sqlString"></param>
        /// <returns></returns>
        public SqlCommand InitalizeCommandObject(SqlCommand sqlCommand, string sqlString)
        {
            sqlCommand = new SqlCommand(sqlString, Connection);
            sqlCommand.CommandType = (sqlString.IndexOf(" ") >= 0) ? CommandType.Text : CommandType.StoredProcedure;
            sqlCommand.CommandTimeout = 1200;    //r  chico  3 27 2015
            return sqlCommand;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlDataAdapter"></param>
        /// <param name="sqlString"></param>
        /// <returns></returns>
        public SqlDataAdapter InitalizeDataAdapterObject(SqlDataAdapter sqlDataAdapter, string sqlString)
        {
            sqlDataAdapter = new SqlDataAdapter(sqlString, Connection);
            sqlDataAdapter.SelectCommand.CommandType = (sqlString.IndexOf(" ") >= 0) ? CommandType.Text : CommandType.StoredProcedure;
            return sqlDataAdapter;
        }

        #endregion
    }
}