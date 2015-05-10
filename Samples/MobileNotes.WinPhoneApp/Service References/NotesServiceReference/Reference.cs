//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.18408
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

// Original file name:
// Generation date: 3/17/2014 1:30:47 AM
namespace MobileNotes.WinPhoneApp.NotesServiceReference
{
    
    /// <summary>
    /// There are no comments for NotesDataContext in the schema.
    /// </summary>
    [global::System.Runtime.Serialization.DataContractAttribute(IsReference=true)]
    public partial class NotesDataContext : global::System.Data.Services.Client.DataServiceContext
    {
        /// <summary>
        /// Initialize a new NotesDataContext object.
        /// </summary>
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        public NotesDataContext(global::System.Uri serviceRoot) : 
                base(serviceRoot, global::System.Data.Services.Common.DataServiceProtocolVersion.V3)
        {
            this.ResolveName = new global::System.Func<global::System.Type, string>(this.ResolveNameFromType);
            this.ResolveType = new global::System.Func<string, global::System.Type>(this.ResolveTypeFromName);
            this.OnContextCreated();
        }
        partial void OnContextCreated();
        /// <summary>
        /// Since the namespace configured for this service reference
        /// in Visual Studio is different from the one indicated in the
        /// server schema, use type-mappers to map between the two.
        /// </summary>
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        protected global::System.Type ResolveTypeFromName(string typeName)
        {
            if (typeName.StartsWith("MobileNotes.Web.Model", global::System.StringComparison.Ordinal))
            {
                return this.GetType().Assembly.GetType(string.Concat("MobileNotes.WinPhoneApp.NotesServiceReference", typeName.Substring(21)), false);
            }
            return null;
        }
        /// <summary>
        /// Since the namespace configured for this service reference
        /// in Visual Studio is different from the one indicated in the
        /// server schema, use type-mappers to map between the two.
        /// </summary>
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        protected string ResolveNameFromType(global::System.Type clientType)
        {
            if (clientType.Namespace.Equals("MobileNotes.WinPhoneApp.NotesServiceReference", global::System.StringComparison.Ordinal))
            {
                return string.Concat("MobileNotes.Web.Model.", clientType.Name);
            }
            return null;
        }
        /// <summary>
        /// There are no comments for Notes in the schema.
        /// </summary>
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        public global::System.Data.Services.Client.DataServiceQuery<Note> Notes
        {
            get
            {
                if ((this._Notes == null))
                {
                    this._Notes = base.CreateQuery<Note>("Notes");
                }
                return this._Notes;
            }
        }
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        private global::System.Data.Services.Client.DataServiceQuery<Note> _Notes;
        /// <summary>
        /// There are no comments for Notes in the schema.
        /// </summary>
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        public void AddToNotes(Note note)
        {
            base.AddObject("Notes", note);
        }
    }
    /// <summary>
    /// There are no comments for MobileNotes.Web.Model.Note in the schema.
    /// </summary>
    /// <KeyProperties>
    /// ID
    /// </KeyProperties>
    [global::System.Data.Services.Common.EntitySetAttribute("Notes")]
    [global::System.Data.Services.Common.DataServiceKeyAttribute("ID")]
    [global::System.Runtime.Serialization.DataContractAttribute(IsReference=true)]
    public partial class Note : global::System.ComponentModel.INotifyPropertyChanged
    {
        /// <summary>
        /// Create a new Note object.
        /// </summary>
        /// <param name="ID">Initial value of ID.</param>
        /// <param name="timeCreated">Initial value of TimeCreated.</param>
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        public static Note CreateNote(int ID, global::System.DateTime timeCreated)
        {
            Note note = new Note();
            note.ID = ID;
            note.TimeCreated = timeCreated;
            return note;
        }
        /// <summary>
        /// There are no comments for Property ID in the schema.
        /// </summary>
        [global::System.Runtime.Serialization.DataMemberAttribute()]
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        public int ID
        {
            get
            {
                return this._ID;
            }
            set
            {
                this.OnIDChanging(value);
                this._ID = value;
                this.OnIDChanged();
                this.OnPropertyChanged("ID");
            }
        }
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        private int _ID;
        partial void OnIDChanging(int value);
        partial void OnIDChanged();
        /// <summary>
        /// There are no comments for Property Text in the schema.
        /// </summary>
        [global::System.Runtime.Serialization.DataMemberAttribute()]
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        public string Text
        {
            get
            {
                return this._Text;
            }
            set
            {
                this.OnTextChanging(value);
                this._Text = value;
                this.OnTextChanged();
                this.OnPropertyChanged("Text");
            }
        }
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        private string _Text;
        partial void OnTextChanging(string value);
        partial void OnTextChanged();
        /// <summary>
        /// There are no comments for Property TimeCreated in the schema.
        /// </summary>
        [global::System.Runtime.Serialization.DataMemberAttribute()]
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        public global::System.DateTime TimeCreated
        {
            get
            {
                return this._TimeCreated;
            }
            set
            {
                this.OnTimeCreatedChanging(value);
                this._TimeCreated = value;
                this.OnTimeCreatedChanged();
                this.OnPropertyChanged("TimeCreated");
            }
        }
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        private global::System.DateTime _TimeCreated;
        partial void OnTimeCreatedChanging(global::System.DateTime value);
        partial void OnTimeCreatedChanged();
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        public event global::System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Data.Services.Design", "1.0.0")]
        protected virtual void OnPropertyChanged(string property)
        {
            if ((this.PropertyChanged != null))
            {
                this.PropertyChanged(this, new global::System.ComponentModel.PropertyChangedEventArgs(property));
            }
        }
    }
}
