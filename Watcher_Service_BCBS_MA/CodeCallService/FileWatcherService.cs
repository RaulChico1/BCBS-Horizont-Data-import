using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Reflection;
using System.IO;
using System.Configuration;
using System.Threading;
namespace CodeCallService
{
	public partial class FileWatcherService : ServiceBase
	{
		List<String> _createdItems;
        
		public FileWatcherService()
		{

#if DEBUG
            System.Diagnostics.Debugger.Launch();
#endif


			InitializeComponent();
            appSets appsets = new appSets();
            appsets.setVars();
			_createdItems = new List<string>();
			
           
		}


		protected override void OnStart(string[] args)
		{
			// Gets called when this service starts			
			base.OnStart(args);
            WinEventLog wL = new WinEventLog();
            try
            {

                _fsWatcher1.EnableRaisingEvents = true;
                _fsWatcher1.Path = ProcessVars.arArrayWatch[0];
                _fsWatcher2.EnableRaisingEvents = true;
                _fsWatcher2.Path = ProcessVars.arArrayWatch[1];
                _fsWatcher3.EnableRaisingEvents = true;
                _fsWatcher3.Path = ProcessVars.arArrayWatch[2];
                _fsWatcher4.EnableRaisingEvents = true;
                _fsWatcher4.Path = ProcessVars.arArrayWatch[3];
                _fsWatcher5.EnableRaisingEvents = true;
                _fsWatcher5.Path = ProcessVars.arArrayWatch[4];
            }
            catch (Exception ex)
            {
               
                wL.WriteEventLogEntry("Reading Manual Recnums: " + ex.Message, 2, 1);
            }
		}



		protected override void OnPause()
		{
			// Gets called when this service pause
			_fsWatcher1.EnableRaisingEvents = false;
            _fsWatcher2.EnableRaisingEvents = false;
            _fsWatcher3.EnableRaisingEvents = false;
            _fsWatcher4.EnableRaisingEvents = false;
            _fsWatcher5.EnableRaisingEvents = false;
			base.OnPause();
		}


		protected override void OnContinue()
		{
			// Gets called when this service resume running			
			base.OnContinue();
			_fsWatcher1.EnableRaisingEvents = true;
            _fsWatcher2.EnableRaisingEvents = true;
            _fsWatcher3.EnableRaisingEvents = true;
            _fsWatcher4.EnableRaisingEvents = true;
            _fsWatcher5.EnableRaisingEvents = true;
		}
		

		protected override void OnStop()
		{
			// Gets called when this service stopped
			_fsWatcher1.EnableRaisingEvents = false;
            _fsWatcher2.EnableRaisingEvents = false;
            _fsWatcher3.EnableRaisingEvents = false;
            _fsWatcher4.EnableRaisingEvents = false;
            _fsWatcher5.EnableRaisingEvents = false;
			_createdItems.Clear();
			base.OnStop();
		}

      
        private void _fsWatcher1_Created(object sender, System.IO.FileSystemEventArgs e)
        {
            FileInfo fInfo = new FileInfo(e.FullPath);
            while (IsFileLocked(fInfo))
            {
                Thread.Sleep(500);
            }
            _createdItems.Add(e.FullPath);
            string outputName = System.Configuration.ConfigurationManager.AppSettings["WatchPathProcess"];
            System.IO.File.Copy(e.FullPath, outputName + e.Name, true);
        }
        private void _fsWatcher2_Created(object sender, System.IO.FileSystemEventArgs e)
        {
            FileInfo fInfo = new FileInfo(e.FullPath);
            while (IsFileLocked(fInfo))
            {
                Thread.Sleep(500);
            }
            _createdItems.Add(e.FullPath);
            string outputName = System.Configuration.ConfigurationManager.AppSettings["WatchPathProcess"];
            System.IO.File.Copy(e.FullPath, outputName + e.Name, true);
        }
        private void _fsWatcher3_Created(object sender, System.IO.FileSystemEventArgs e)
        {
            FileInfo fInfo = new FileInfo(e.FullPath);
            while (IsFileLocked(fInfo))
            {
                Thread.Sleep(500);
            }
            _createdItems.Add(e.FullPath);
            string outputName = System.Configuration.ConfigurationManager.AppSettings["WatchPathProcess"];
            System.IO.File.Copy(e.FullPath, outputName + e.Name, true);
        }
        private void _fsWatcher4_Created(object sender, System.IO.FileSystemEventArgs e)
        {
            FileInfo fInfo = new FileInfo(e.FullPath);
            while (IsFileLocked(fInfo))
            {
                Thread.Sleep(500);
            }
            _createdItems.Add(e.FullPath);
            string outputName = System.Configuration.ConfigurationManager.AppSettings["WatchPathProcess"];
            System.IO.File.Copy(e.FullPath, outputName + e.Name, true);
        }
        private void _fsWatcher5_Created(object sender, System.IO.FileSystemEventArgs e)
        {
            WinEventLog wL = new WinEventLog();
            try
            {
                FileInfo fInfo = new FileInfo(e.FullPath);
                while (IsFileLocked(fInfo))
                {
                    Thread.Sleep(500);
                }
                _createdItems.Add(e.FullPath);
                ManualRecnums manualRecs = new ManualRecnums();
                manualRecs.evaluate_TXT(e.FullPath);
            }
            catch (Exception ex)
            {
                wL.WriteEventLogEntry("Reading Manual Recnums: " + ex.Message, 2, 1);
            }
            //string outputName = System.Configuration.ConfigurationManager.AppSettings["WatchPathProcess"];
            //System.IO.File.Copy(e.FullPath, outputName + e.Name, true);
        }
        static bool IsFileLocked(FileInfo file)
        {
            FileStream stream = null;
            try
            {
                stream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                return true;
            }
            finally
            {
                if (stream != null)
                    stream.Close();
            }
            return false;
        }

        //private void _fsWatcher_Created(object sender, System.IO.FileSystemEventArgs e)
        //{
        //    _createdItems.Add(e.FullPath);
        //    string outputName = System.Configuration.ConfigurationManager.AppSettings["WatchPathProcess"];
        //    System.IO.File.Copy(e.FullPath, outputName + e.Name, true);
        //}
	}
}
