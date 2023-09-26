using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
// For File.Exists, Directory.Exists
using System.IO;
using System.Collections;
using System.Diagnostics;
using System.Net;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using System.Data;
using System.Net.Http;
using System.Net.Mail;
using System.ComponentModel;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync;

namespace LoadBackupAndRestore
{
    
    public class Program
    {
        //To run the program, e.g. issue LoadBackupAndRestore.exe at the command line from the folder where the program resides
        // run on 192.168.1.19 server where cdb_Wilmington (cieTrade database resides)

        //public static void Main(string[] args)
        static async Task Main()
        {
            string SQLServerName = "WPGTESTSQL";
            string DatabaseToRestore = "cdb_Wilmington_stage";  //or "cdb_Wilmington_08_03"; 
            string TargetDatabase = "cdb_Wilmington";
            string TestDatabase = "cdb_Wilmington_Test";
            string UtilityName = "sqlcmd.exe";
            string BaseFolder = @"C:\Users\Admin\";
            string Backupfolder = BaseFolder + @"Downloads\";
            string MoveToFolder = BaseFolder + @"Archive\";
            string LogsFolder = BaseFolder + @"Logs\";
            string EmailsFile = BaseFolder + @"Emails\EmailsList.txt";
            //Before running the program, check that this location is accessible via SSMS when trying to restore database interactively, change it accordingly, if needed.
            string RunFromFolder = @"C:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\Backup\";
            string pattern = "*.bak";
            string NewFileName = RunFromFolder + "cdb_Wilmington_backup.bak";
            //If Backupfolder did not exist and was restored during program execution, copy script file(s) from C:\Users\Admin\ folder (must exist on the PC) before running the program next time
            string RestoreDatabaseScriptLocation = Backupfolder + "SQLQuerycdb_Wilmington_Database_Restore_Stage.sql"; //"SQLQuerycdb_Wilmington_Database_Restore_Test.sql"; SQLQuerycdb_Wilmington_Database_Restore_Stage.sql
            string RestoreDatabaseLogFile = LogsFolder + "DatabaseRestoreLog.txt";
            string url = "https://portals.cietrade.com/WilmingtonGroup/DatabaseBackups/";
            string UpdateTablesFromStage = Backupfolder + "cdb_Wilmington_TablesUpdate2.sql";
            string CietradeBackupFile;
            string DownloadFileName;
            string BackupFileNameFromPortal;
            int index;
            bool RestoreOK = true;
            string content;
            string smtpserver = "wpcdc01.wilmington.local"; //"192.168.1.30";
            int port = 25; //587;
            string result;
            string FromAddress = ""; //e.g. "cdbrestore@wilmingtonpaper.com";
            string ToAddress; // e.g. "iraspin@wilmingtonpaper.com,jcastoire@recyclingmr.com";

            string CurrentUser = ""; 
            string CurrentPass = ""; 

            DateTime localDate;

            string year;
            string month;
            string day;

            FileStream fs;
            DirectoryInfo di;

            year = DateTime.Today.AddDays(-1).Year.ToString();
            month = DateTime.Today.AddDays(-1).Month.ToString();
            day = DateTime.Today.AddDays(-1).Day.ToString();

            if (month.Length == 1)
                month = "0" + month;

            if (day.Length == 1)
                day = "0" + day;

            if (!Directory.Exists(MoveToFolder))
            {
                di = Directory.CreateDirectory(MoveToFolder);
            }


            if (!Directory.Exists(LogsFolder))
            {
                di = Directory.CreateDirectory(LogsFolder);
            }

            if (!Directory.Exists(Backupfolder))
            {
                di = Directory.CreateDirectory(Backupfolder);
            }

            ToAddress = File.ReadAllText(EmailsFile);

            string todaysFileName = "cdb_Wilmington_backup_" + year + "_" + month + "_" + day + "_";

            GetUser(SQLServerName , ref CurrentUser, ref CurrentPass);

            FromAddress = CurrentUser;

            using HttpClient client = new HttpClient();
            {
                using HttpResponseMessage response = await client.GetAsync(url);
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        using StreamWriter sw = File.AppendText(RestoreDatabaseLogFile);
                        {
                            result = "\n" + url + "is not available on " + DateTime.Now.ToString() + "Check portal, contact Cietrade if needed.";
                            Console.WriteLine(result);
                            sw.WriteLine(result);
                            sw.Close();
                        }

                        SendEmailMessage(smtpserver, FromAddress, ToAddress, port, DatabaseToRestore + " restore error"
                        , result + "\nSee attached", RestoreDatabaseLogFile, CurrentUser, CurrentPass);

                        return;
                    }
                    response.EnsureSuccessStatusCode();
                    content = await response.Content.ReadAsStringAsync();

                    if (!content.Contains(todaysFileName))
                    {
                        using (StreamWriter sw = File.AppendText(RestoreDatabaseLogFile))
                        {
                            result = "\nThere is no today backup file. Check portal, contact Cietrade if needed. " + DateTime.Today.ToString();
                            Console.WriteLine(result);
                            sw.WriteLine(result);
                            sw.Close();
                        }

                        SendEmailMessage(smtpserver, FromAddress, ToAddress, port, DatabaseToRestore + " restore error", 
                            result + "\nSee attached", RestoreDatabaseLogFile, CurrentUser, CurrentPass);
                        return;
                    }
                }
            }

            index = content.IndexOf(todaysFileName);

            BackupFileNameFromPortal = content.Substring(index, 47) + ".bak";

            CietradeBackupFile = url + BackupFileNameFromPortal;

            DownloadFileName = Backupfolder + BackupFileNameFromPortal;

            // Get the backup file from Cietrade portal

            using (WebClient webclient = new WebClient())
            {
                webclient.DownloadFile(CietradeBackupFile, DownloadFileName);
                webclient.DownloadFileCompleted += DownloadCompleted;
            }

            //GetCietradeFile(CietradeBackupFile, DownloadFileName, powershell);

            try
            {
                if (!File.Exists(RestoreDatabaseLogFile))
                {
                    fs = File.Create(RestoreDatabaseLogFile);
                    fs.Close();
                }

                string[] dirs = Directory.GetFiles(Backupfolder, pattern);

                Console.WriteLine("\nThe number of matching bak files in " + Backupfolder + " folder is {0}. On {1}, should be 1. Remove unneccessary files from " + Backupfolder + ", except the latest one.", dirs.Length, DateTime.Now.ToString());

                if (dirs.Length > 1)
                {
                    using (StreamWriter sw = File.AppendText(RestoreDatabaseLogFile))
                    {
                        sw.WriteLine("\n-----------------------------------------------------------------------------------------------------------------------------------------------------");
                        //sw.WriteLine("The number of matching bak files in " + Backupfolder + " folder is {0}. On {1}, should be 1. Remove unneccessary files from," + Backupfolder + " except the latest one.\n", dirs.Length, DateTime.Now.ToString());
                        result = "The number of matching bak files in " + Backupfolder + " folder is " + dirs.Length.ToString() + ". On " + DateTime.Now.ToString() + ", should be 1. Remove unneccessary files from " + Backupfolder + " except the latest one and re-run the program.";
                        sw.WriteLine(result);
                        sw.Close();
                        SendEmailMessage(smtpserver, FromAddress, ToAddress, port, DatabaseToRestore + " restore error", 
                            result + "\nSee attached", RestoreDatabaseLogFile, CurrentUser, CurrentPass);
                    }
                    return;
                }

                if (dirs.Length == 1)
                {
                    foreach (string dir in dirs)
                    {
                        //e.g. Backup file name - cdb_Wilmington_backup_2023_08_02_220021_6477080.bak

                        File.Copy(dir, NewFileName, true); //NewFileName - this is the file to restore the database from

                        // Do Restore here from NewFileName...
                        RunScriptToRestoreDatabase(RestoreDatabaseScriptLocation, SQLServerName, UtilityName, RestoreDatabaseLogFile);

                        // Check database restore on SQL Server
                        RestoreOK = CheckDatabaseRestoreOnSQLServer(SQLServerName, DatabaseToRestore, RestoreOK);

                        localDate = DateTime.Now;

                        using (StreamWriter sw = File.AppendText(RestoreDatabaseLogFile))
                        {
                            if (RestoreOK)
                            {
                                RunUpdateTablesFromStage(UpdateTablesFromStage, SQLServerName, UtilityName);

                                sw.WriteLine("File: {0} was downloaded to: {1}", CietradeBackupFile, DownloadFileName);
                                result = DatabaseToRestore + " database was restored from " + CietradeBackupFile + " on " + localDate.ToString() + " and tables in " + TargetDatabase + " were updated." ;
                                sw.WriteLine(result);
                                sw.Close();

                                SendEmailMessage(smtpserver, FromAddress, ToAddress, port, DatabaseToRestore + " restore", 
                                    result + "\nSee attached", RestoreDatabaseLogFile, CurrentUser, CurrentPass);
                            }
                            else 
                            {
                                sw.WriteLine("\nFile: {0} was downloaded to: {1}", CietradeBackupFile, DownloadFileName);
                                result = " Troubleshoot, rerun the program. " + DatabaseToRestore + " database was not restored from " + CietradeBackupFile + " on " + localDate.ToString();
                                sw.WriteLine(result);
                                sw.Close();

                                SendEmailMessage(smtpserver, FromAddress, ToAddress, port, DatabaseToRestore + " restore error", 
                                    result + "\nSee attached", RestoreDatabaseLogFile, CurrentUser, CurrentPass);
                            }
                        }

                        string oldfilename = Path.GetFileName(dir);

                        if (!File.Exists(@MoveToFolder + oldfilename))
                            File.Move(dir, @MoveToFolder + oldfilename); //move original backup file to archive folder
                        else
                            File.Delete(dir);
                    }
                }
                else if
                    (dirs.Length == 0)
                {
                    using (StreamWriter sw = File.AppendText(RestoreDatabaseLogFile))
                    {
                        result = "\nTroubleshoot, rerun the program, the Cietrade bak file was not downloaded" + " on " + DateTime.Today.ToString();
                        sw.WriteLine(result);
                        sw.Close();
                        SendEmailMessage(smtpserver, FromAddress, ToAddress, port, DatabaseToRestore + " restore error", 
                        result + "\nSee attached", RestoreDatabaseLogFile, CurrentUser, CurrentPass);
                    }
                }
            }

            catch (Exception e)
            {
                Console.WriteLine("The process failed: {0}", e.ToString());
            }

        }
        
        static void DownloadCompleted(object sender, AsyncCompletedEventArgs e)
        {
            if (e.Error != null)
            {
                Console.WriteLine("File did not download from portal.");
                return;
            }
            else
                Console.WriteLine("Downloaded successfully from portal.");
        }
        static void RunScriptToRestoreDatabase(string script, string server, string UtilityName, string RestoreDatabaseLogFile)
        {
            using (Process RestoreDatabase = new Process())
            {
                RestoreDatabase.StartInfo.FileName = UtilityName;
                RestoreDatabase.StartInfo.Arguments = "-S " + server + " -i " + script;
                RestoreDatabase.StartInfo.UseShellExecute = false;
                RestoreDatabase.StartInfo.RedirectStandardOutput = true;

                RestoreDatabase.Start();

                using (StreamWriter sw = File.AppendText(RestoreDatabaseLogFile))
                {
                    sw.WriteLine("\n" + DateTime.Now.ToString()); 
                    sw.WriteLine(RestoreDatabase.StandardOutput.ReadToEnd());
                    sw.Close();
                }

                Console.WriteLine(RestoreDatabase.StandardOutput.ReadToEnd());

                RestoreDatabase.WaitForExit();
            }
        }
        static void RunUpdateTablesFromStage(string script, string server, string UtilityName)
        {
            using (Process RestoreDatabase = new Process())
            {
                RestoreDatabase.StartInfo.FileName = UtilityName;
                RestoreDatabase.StartInfo.Arguments = "-S " + server + " -i " + script;
                RestoreDatabase.StartInfo.UseShellExecute = false;
                RestoreDatabase.StartInfo.RedirectStandardOutput = true;

                RestoreDatabase.Start();

                //RestoreDatabase.WaitForExit();
            }
        }

        static void RestoreDatabase(string server, string DatabaseToRestore, string CietradeBackupFile, string UtilityName)
        {
            var startInfo = new ProcessStartInfo()
            {
                FileName = UtilityName,
                Arguments = "-S " + server + "USE master;RESTORE DATABASE " + DatabaseToRestore + "FROM URL = N'" + CietradeBackupFile + "'",
                UseShellExecute = false
            };
            Process.Start(startInfo);
        }

        static void GetCietradeFile(string CietradeBackupFile, string DownloadFileName, string powershell)
        {
            var startInfo = new ProcessStartInfo()
            {
                FileName = powershell,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                Arguments = "Invoke-WebRequest -Uri " + CietradeBackupFile + " -OutFile " + DownloadFileName
            };
            Process.Start(startInfo).WaitForExit();
        }

        static bool CheckDatabaseRestoreOnSQLServer(string SQLServerName, string DatabaseToRestore, bool RestoreOK)
        {

            string connectionString = "Server=" + SQLServerName + ";Database=msdb; Integrated Security=SSPI;";
            //string SQLString = "SELECT MAX(CAST([restore_date] AS DATE)) Last_Restore_Date FROM [msdb].[dbo].[restorehistory] WHERE [destination_database_name] = '" + DatabaseToRestore + "'";
            string SQLString = "SELECT TOP(1) restore_date AS Last_Restore_Date FROM [msdb].[dbo].[restorehistory] WHERE [destination_database_name] = '" + DatabaseToRestore + "' ORDER BY restore_date DESC";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlDataAdapter da = new SqlDataAdapter(SQLString, connection);
                DataTable dt = new DataTable();
                da.Fill(dt);

                foreach (DataRow row in dt.Rows)
                {
                    DateTime RestoreDateTimeFromServer = (DateTime)row["Last_Restore_Date"];
                    if (DateTime.Now.Date != RestoreDateTimeFromServer.Date)
                        RestoreOK = false;
                }
            }

            return RestoreOK;
        }

        static void GetUser(string SQLServerName, ref string CurrentUser, ref string CurrentPass)
        {
            string connectionString = "Server=" + SQLServerName + ";Database=Util; Integrated Security=SSPI;";
            string SQLString = "SELECT [user], dbo.Decrypt([password]) [password] FROM [Util].[dbo].[SMTPUsers];";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlDataAdapter da = new SqlDataAdapter(SQLString, connection);
                DataTable dt = new DataTable();
                da.Fill(dt);

                foreach (DataRow row in dt.Rows)
                {
                    CurrentUser = (string)row["user"];
                    CurrentPass = (string)row["password"];
                }
            }
        }

        public static void SendEmailMessage(string smtpserver, string from, string to, int port, string subject, string body, string attached, string CurrentUser, string CurrentPassword)
        {
            MailMessage message = new MailMessage(from, to);

            System.Net.Mail.Attachment attachment;
            attachment = new System.Net.Mail.Attachment(attached);
            message.Attachments.Add(attachment);

            message.Subject = subject;

            message.Body = body;

            SmtpClient client = new SmtpClient(smtpserver, port);
            
            client.Host = smtpserver;

            client.Port = port;

            client.EnableSsl = true;

            NetworkCredential MyCredentials = new NetworkCredential(CurrentUser, CurrentPassword);

            client.Credentials = MyCredentials;

            try
            {
                client.Send(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception caught in SendEmailMessage: {0}", ex.ToString());
            }
        }
        public static async Task SyncTablesAsync(string SQLServerName, string DatabaseToRestore, string TargetDatabase)
        {
            int arrayLength = 0;
            var tables = new string[arrayLength];

            using (SqlConnection connection = new SqlConnection("Server=" + SQLServerName + ";Database=" + DatabaseToRestore + "; Integrated Security=SSPI;"))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = @"SELECT s.name, o.name  FROM sys.objects o WITH(NOLOCK) JOIN sys.schemas s WITH(NOLOCK)
                                                 ON o.schema_id = s.schema_id
                                                 WHERE o.is_ms_shipped = 0 AND RTRIM(o.type) = 'U'
                                                 ORDER BY s.name ASC, o.name ASC";

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string schemaName = reader.GetString(0);
                            string tableName = reader.GetString(1);

                            arrayLength = arrayLength + 1;
                            Array.Resize(ref tables, arrayLength);

                            tables[arrayLength - 1] = schemaName + "." + tableName;
                        }
                    }
                }
            }

            string serverConnectionString = "Server=" + SQLServerName + ";Database=" + DatabaseToRestore + "; Integrated Security=SSPI;";
            string clientConnectionString = "Server=" + SQLServerName + ";Database=" + TargetDatabase + "; Integrated Security=SSPI;";

            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Sync agent
            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

            var resultSync = await agent.SynchronizeAsync();

            Console.WriteLine(resultSync);
        }
    }
}