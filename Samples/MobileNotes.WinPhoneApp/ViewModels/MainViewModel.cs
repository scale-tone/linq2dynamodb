using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Linq;
using MobileNotes.Common;
using MobileNotes.WinPhoneApp.Common;
using MobileNotes.WinPhoneApp.NotesServiceReference;

namespace MobileNotes.WinPhoneApp.ViewModels
{
    /// <summary>
    /// The ViewModel for the list of notes
    /// </summary>
    public class MainViewModel : BaseViewModel
    {
        #region Public Properties

        /// <summary>
        /// The collection of notes displayed
        /// </summary>
        public DataServiceCollection<Note> Notes { get; private set; }

        private bool _showTodaysNotesOnly;
        public bool ShowTodaysNotesOnly
        {
            get { return this._showTodaysNotesOnly; }
            set
            {
                this._showTodaysNotesOnly = value;
                this.LoadData();

                this.NotifyPropertyChanged(() => this.ShowTodaysNotesOnly);
            }
        }

        private bool _isUiEnabled;
        public bool IsUiEnabled
        {
            get { return this._isUiEnabled; }
            private set
            {
                this._isUiEnabled = value;
                this.NotifyPropertyChanged(() => this.IsUiEnabled);
                this.OnUiEnabledChanged.FireSafely(this._isUiEnabled);
            }
        }

        public bool IsDataLoaded { get; private set; }

        #endregion

        #region Events

        public event Action OnChangesSaved;
        public event Action<Exception> OnError;

        /// <summary>
        /// This event is required because Application Bar buttons do not support data binding
        /// </summary>
        public event Action<bool> OnUiEnabledChanged;

        #endregion

        #region Public Methods

        /// <summary>
        /// Stores the user's authentication token internally and reloads notes from server using that token
        /// </summary>
        public void SetAuthenticationToken(string token)
        {
            this._authenticationToken = token;

            if (string.IsNullOrEmpty(this._authenticationToken))
            {
                this._context = null;
                this.Notes = null;
                this.NotifyPropertyChanged(() => this.Notes);

                this.IsDataLoaded = false;
                this.IsUiEnabled = false;
            }
            else
            {
                this.LoadData();
            }
        }

        /// <summary>
        /// Adds a note and sends changes to server
        /// </summary>
        /// <param name="text"></param>
        public void AddNote(string text)
        {
            var note = new Note
            {
                ID = this.GetNewNoteId(),
                Text = text,
                TimeCreated = DateTime.Now
            };

            this.Notes.Add(note);

            this.SaveChanges();
        }

        /// <summary>
        /// Removes selected notes and sends changes to server
        /// </summary>
        /// <param name="notesToRemove"></param>
        public void RemoveNotes(IEnumerable<Note> notesToRemove)
        {
            foreach (var note in notesToRemove)
            {
                this.Notes.Remove(note);
            }
            this.SaveChanges();
        }

        #endregion

        #region Private Properties

        //TODO: put your server name into this URI
        public const string NotesDataServiceUri = "http://samantha-jr/MobileNotes.Web/Services/NotesDataService.svc";

        private NotesDataContext _context;

        private string _authenticationToken;

        #endregion

        #region Private Methods

        /// <summary>
        /// Reloads notes from server
        /// </summary>
        private void LoadData()
        {
            this.IsDataLoaded = false;
            this.IsUiEnabled = false;

            // preparing a data context
            this._context = new NotesDataContext(new Uri(NotesDataServiceUri));

            // passing the authentication token obtained from Microsoft Account via the Authorization header
            this._context.SendingRequest += (sender, args) =>
            {
                // let our header look a bit custom
                args.RequestHeaders["Authorization"] = Constants.AuthorizationHeaderPrefix + this._authenticationToken;
            };

            var notes = new DataServiceCollection<Note>(this._context);
            notes.LoadCompleted += (sender, args) =>
            {
                if (this.Notes.Continuation != null)
                {
                    this.Notes.LoadNextPartialSetAsync();
                    return;
                }

                if (args.Error == null)
                {
                    this.IsDataLoaded = true;
                }
                else
                {
                    this.OnError.FireSafely(args.Error);
                }

                this.IsUiEnabled = true;
            };

            // Defining a query
            var query =
                from note in this._context.Notes
                orderby note.TimeCreated descending
                select note;

            // Demonstrating LINQ query dynamic generation.
            notes.LoadAsync
            (
                this._showTodaysNotesOnly
                ?
                // This will be translated into DynamoDb Query on the server, like this:
                // DynamoDb  index query: SELECT * FROM Note WHERE TimeCreated GreaterThan <some DateTime> AND UserId Equal <some userId> ORDER BY TimeCreated DESC. Index name: TimeCreatedIndex
                query.Where(note => note.TimeCreated > DateTime.Today)
                :
                query
            );

            this.Notes = notes;
            this.NotifyPropertyChanged(() => this.Notes);
        }

        /// <summary>
        /// Saves added and removed notes asynchronously
        /// </summary>
        private void SaveChanges()
        {
            this.IsUiEnabled = false;

            this._context.BeginSaveChanges
            (
                /* 
                    There's a bug in Microsoft.Data.Services.Client: 
                    If some GUI ItemsControl is bound to DataServiceCollection, then calling DataContext.EndSaveChanges()
                    in a worker thread causes an UnauthorizedAccessException (that's because collection bindings are 
                    not thread-safe in Silverlight/WinPhone).
                    So, we have to manually marshal that call to the GUI thread with SyncContext.Post().
                */
                iar => this.SyncContext.Post(_ =>
                {
                    try
                    {
                        this._context.EndSaveChanges(iar);
                        this.OnChangesSaved.FireSafely();
                    }
                    catch (Exception ex)
                    {
                        this.OnError.FireSafely(ex);
                    }

                    this.IsUiEnabled = true;

                }, null),
                null
            );
        }

        /// <summary>
        /// This is of course a completely wrong way to get an ID for a new Note
        /// </summary>
        /// <returns></returns>
        private int GetNewNoteId()
        {
            if (this.Notes.Count <= 0)
            {
                return 1;
            }

            int newId = this.Notes.Max(n => n.ID) + 1;
            return newId;
        }

        #endregion
    }
}