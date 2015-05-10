using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
using Microsoft.Live;
using Microsoft.Live.Controls;
using Microsoft.Phone.Controls;
using Microsoft.Phone.Shell;
using MobileNotes.Common;
using MobileNotes.WinPhoneApp.NotesServiceReference;

namespace MobileNotes.WinPhoneApp
{
    public partial class MainPage : PhoneApplicationPage
    {
        private readonly ApplicationBarIconButton _addNoteAppButton;
        private readonly ApplicationBarIconButton _selectAppButton;
        private readonly ApplicationBarIconButton _removeNotesAppButton;

        // Constructor
        public MainPage()
        {
            InitializeComponent();

            this.SigninButton.ClientId = Constants.LiveClientId;

            // this is a common way for refreshing Application Bar buttons
            this._addNoteAppButton = (ApplicationBarIconButton)ApplicationBar.Buttons[0];
            this._selectAppButton = (ApplicationBarIconButton)ApplicationBar.Buttons[1];
            this._removeNotesAppButton = (ApplicationBarIconButton)ApplicationBar.Buttons[2];

            this.DataContext = App.ViewModel;
        }

        protected override void OnNavigatedTo(NavigationEventArgs e)
        {
            App.ViewModel.OnChangesSaved += this.ViewModel_OnChangesSaved;
            App.ViewModel.OnError += this.ViewModel_OnError;
            App.ViewModel.OnUiEnabledChanged += this.ViewModel_OnUiEnabledChanged;

            this.RefreshApplicationBar();
        }

        protected override void OnNavigatedFrom(NavigationEventArgs e)
        {
            App.ViewModel.OnChangesSaved -= this.ViewModel_OnChangesSaved;
            App.ViewModel.OnError -= this.ViewModel_OnError;
            App.ViewModel.OnUiEnabledChanged -= this.ViewModel_OnUiEnabledChanged;
        }

        private void ViewModel_OnChangesSaved()
        {
            MessageBox.Show("Changes saved successfully!");
        }

        private void ViewModel_OnError(Exception ex)
        {
            MessageBox.Show(ex.ToString());
        }

        private void ViewModel_OnUiEnabledChanged(bool obj)
        {
            this.RefreshApplicationBar();
        }

        private void MainListBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            this.RefreshApplicationBar();
        }

        private void MainListBox_IsSelectionEnabledChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            this.RefreshApplicationBar();
        }

        private void Select_Click(object sender, EventArgs e)
        {
            this.MainListBox.IsSelectionEnabled = true;
        }

        private void AddNote_Click(object sender, EventArgs e)
        {
            // let the user enter her note in a popup window

            var noteTextBox = new TextBox { Text = "My New Note" };

            var messageBox = new CustomMessageBox
            {
                Caption = "Note Text",
                Content = noteTextBox,
                LeftButtonContent = "OK",
                RightButtonContent = "Cancel"
            };

            messageBox.Dismissed += (s, args) =>
            {
                switch (args.Result)
                {
                    case CustomMessageBoxResult.LeftButton:

                        App.ViewModel.AddNote(noteTextBox.Text);

                    break;
                    case CustomMessageBoxResult.RightButton:
                    case CustomMessageBoxResult.None:
                    break;
                }
            };

            messageBox.Show();
        }

        private void RemoveNote_Click(object sender, EventArgs e)
        {
            App.ViewModel.RemoveNotes(this.MainListBox.SelectedItems.Cast<Note>().ToArray());

            this.MainListBox.IsSelectionEnabled = false;
        }

        private void SignInButton_OnSessionChanged(object sender, LiveConnectSessionChangedEventArgs e)
        {
            // As soon as user is logged in, passing her AuthenticationToken to the ViewModel to enable interaction with the service
            App.ViewModel.SetAuthenticationToken
            (
                e.Status == LiveConnectSessionStatus.Connected
                ? 
                e.Session.AuthenticationToken
                : 
                string.Empty
            );

            if (e.Error != null)
            {
                Dispatcher.BeginInvoke(() => MessageBox.Show(e.Error.Message));
            }
        }

        private void RefreshApplicationBar()
        {
            while (this.ApplicationBar.Buttons.Count > 0)
            {
                this.ApplicationBar.Buttons.RemoveAt(0);
            }

            if (!App.ViewModel.IsUiEnabled)
            {
                return;
            }

            if (App.ViewModel.IsDataLoaded)
            {
                this.ApplicationBar.Buttons.Add(this._addNoteAppButton);
            }

            if (this.MainListBox.IsSelectionEnabled)
            {
                if (this.MainListBox.SelectedItems.Count > 0)
                {
                    this.ApplicationBar.Buttons.Add(this._removeNotesAppButton);
                }
            }
            else if (App.ViewModel.Notes.Count > 0)
            {
                this.ApplicationBar.Buttons.Add(this._selectAppButton);
            }
        }
    }
}