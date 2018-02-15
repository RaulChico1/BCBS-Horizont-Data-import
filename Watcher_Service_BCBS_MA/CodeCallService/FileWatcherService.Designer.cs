
namespace CodeCallService
{
	partial class FileWatcherService
	{
		/// <summary> 
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.IContainer components = null;

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		/// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
		protected override void Dispose(bool disposing)
		{
			if (disposing && (components != null))
			{
				components.Dispose();
			}
			base.Dispose(disposing);
		}

		#region Component Designer generated code

		/// <summary> 
		/// Required method for Designer support - do not modify 
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
            this._fsWatcher1 = new System.IO.FileSystemWatcher();
            this._fsWatcher2 = new System.IO.FileSystemWatcher();
            this._fsWatcher3 = new System.IO.FileSystemWatcher();
            this._fsWatcher4 = new System.IO.FileSystemWatcher();
            this._fsWatcher5 = new System.IO.FileSystemWatcher();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher2)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher3)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher4)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher5)).BeginInit();
            // 
            // _fsWatcher1
            // 
            this._fsWatcher1.EnableRaisingEvents = true;
            this._fsWatcher1.IncludeSubdirectories = true;
            this._fsWatcher1.Created += new System.IO.FileSystemEventHandler(this._fsWatcher1_Created);
            // 
            // _fsWatcher2
            // 
            this._fsWatcher2.EnableRaisingEvents = true;
            this._fsWatcher2.IncludeSubdirectories = true;
            this._fsWatcher2.Created += new System.IO.FileSystemEventHandler(this._fsWatcher2_Created);
            // 
            // _fsWatcher3
            // 
            this._fsWatcher3.EnableRaisingEvents = true;
            this._fsWatcher3.IncludeSubdirectories = true;
            this._fsWatcher3.Created += new System.IO.FileSystemEventHandler(this._fsWatcher3_Created);
            // 
            // _fsWatcher4
            // 
            this._fsWatcher4.EnableRaisingEvents = true;
            this._fsWatcher4.IncludeSubdirectories = true;
            this._fsWatcher4.Created += new System.IO.FileSystemEventHandler(this._fsWatcher4_Created);
            // 
            // _fsWatcher5
            // 
            this._fsWatcher5.EnableRaisingEvents = true;
            this._fsWatcher5.IncludeSubdirectories = true;
            this._fsWatcher5.Created += new System.IO.FileSystemEventHandler(this._fsWatcher5_Created);
            // 
            // FileWatcherService
            // 
            this.CanPauseAndContinue = true;
            this.ServiceName = "BCBS_MA_Watcher";
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher2)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher3)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher4)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this._fsWatcher5)).EndInit();

		}

		#endregion

		private System.IO.FileSystemWatcher _fsWatcher1;
        private System.IO.FileSystemWatcher _fsWatcher2;
        private System.IO.FileSystemWatcher _fsWatcher3;
        private System.IO.FileSystemWatcher _fsWatcher4;
        private System.IO.FileSystemWatcher _fsWatcher5;
        /* DEFINE WATCHER EVENTS... */
        /// <summary>
        /// Event occurs when the contents of a File or Directory are changed
        /// </summary>
        //private void _fsWatcher_Changed(object sender,
        //                System.IO.FileSystemEventArgs e)
        //{
        //    //code here for newly changed file or directory
        //}
        ///// <summary>
        ///// Event occurs when the a File or Directory is created
        ///// </summary>
        //private void _fsWatcher_Created(object sender,
        //                System.IO.FileSystemEventArgs e)
        //{
        //     string time1R = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
        //            var tR = Task.Run(async delegate
        //            {
        //                await Task.Delay(1000 * 60 * 1);
        //                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
        //            });
        //            tR.Wait();

        //    string outputName = System.Configuration.ConfigurationManager.AppSettings["WatchPathProcess"] ;
        //    System.IO.File.Copy(e.FullPath, outputName + e.Name,true);
        //}
        ///// <summary>
        ///// Event occurs when the a File or Directory is deleted
        ///// </summary>
        //private void _fsWatcher_Deleted(object sender,
        //                System.IO.FileSystemEventArgs e)
        //{
        //    //code here for newly deleted file or directory
        //}
        ///// <summary>
        ///// Event occurs when the a File or Directory is renamed
        ///// </summary>
        //private void _fsWatcher_Renamed(object sender,
        //                System.IO.RenamedEventArgs e)
        //{
        //    //code here for newly renamed file or directory
        //}
	}
}
