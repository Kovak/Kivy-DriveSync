__version__ = '1.0'
import kivy
from kivy.app import App
from kivy.uix.widget import Widget
from kivy.uix.button import Button
import httplib2
import os
from os import listdir
from os.path import isfile, join
import time
import datetime
from threading import Thread
from kivy.uix.carousel import Carousel
from kivy.compat import queue as Queue
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage
from kivy.uix.image import AsyncImage, Image
from kivy.core.image import ImageData
from kivy.clock import Clock
from kivy.storage.jsonstore import JsonStore
from kivy.properties import ListProperty, NumericProperty
from dbinterface import DBInterface
from functools import partial
import json
from kivy.uix.popup import Popup
from kivy.uix.floatlayout import FloatLayout
from flat_kivy.utils import construct_target_file_name

storage = Storage(construct_target_file_name('drive_key.secret', __file__))
with open(construct_target_file_name('client_secret.secret', __file__), 'rb') as client_secret:
    data = json.load(client_secret)
    CLIENT_ID = data[u'installed'][u'client_id']
    CLIENT_SECRET = data[u'installed'][u'client_secret']
    REDIRECT_URI = data[u'installed'][u'redirect_uris'][0]


# Check https://developers.google.com/drive/scopes for all available scopes
OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive.readonly'

def start_drive_thread(folders_to_track, pyfile):
    global drive_main_thread
    drive_main_thread = DriveMainThread(folders_to_track, 1, pyfile)
    drive_main_thread.setDaemon(True)
    drive_main_thread.start()

class DriveProgressTracker(Widget):
    largest_change = NumericProperty(1.)
    last_change_id = NumericProperty(0.)
    start_change_id = NumericProperty(None)
    change_multiplier = NumericProperty(0.)

    def on_largest_change(self, instance, value):
        print('largest change', value)
        self.calculate_change_mult()

    def calculate_change_mult(self):
        if self.start_change_id is not None and not (
            self.start_change_id == self.largest_change): 
            print('last/start/largest', self.last_change_id, 
                self.start_change_id, 
                self.largest_change)
            self.change_multiplier = float(
                self.last_change_id - self.start_change_id)/float(
                self.largest_change - self.start_change_id)
        else:
            self.change_multiplier = 0.

    def on_last_change_id(self, instance, value):
        print('last change', value)
        self.calculate_change_mult()

    def on_start_change_id(self, instance, value):
        print('start_change', value)
        self.calculate_change_mult()

    def on_change_multiplier(self, instance, value):
        print('new mult', value)
        
class DriveCarousel(Carousel):
    folders_to_track = ListProperty(['TestImages'])
    
    def __init__(self, **kwargs):
        self.files_being_tracked = {}
        self.pyfile = pyfile = kwargs.get('pyfile', __file__)
        self.drive_data = DBInterface(os.path.dirname(
            os.path.abspath(pyfile))+'/drive_data/', 'drive_files')
        super(DriveCarousel, self).__init__(**kwargs)
        folders_to_track = kwargs.get('folders_to_track', 
            self.folders_to_track)
        self.set_folders_to_track(folders_to_track)
        
        self.folder_observers = {}
        self.popup = popup = Popup()
        self.progress_tracker = progress_tracker = DriveProgressTracker()
       
        #Clock.schedule_interval(self.retrieve_all_changes, 30.)
        #Clock.schedule_interval(self.notify_folder_observers, 15.)
        self.schedule_change(0.)
        Clock.schedule_interval(self.update_carousel, 5.)
        Clock.schedule_interval(self.schedule_change, 30.)

    def update_carousel(self, dt):
        self.load_next()

    def set_folders_to_track(self, folders_to_track):
        base_path = os.path.dirname(os.path.abspath(self.pyfile))
        global drive_main_thread
        self.folders_to_track = folders_to_track
        for each in folders_to_track:
            path = base_path + '/' + each + '/'
            #self.drive_main_thread.ensure_dir(path)
            drive_main_thread.add_drive_widget(self, path)
            self.track_existing_files(path)
    
    def schedule_change(self, dt):
        global drive_main_thread
        drive_main_thread.update_queue.put(True)

    def track_existing_files(self, path):
        files = [join(path,f) for f in listdir(path) if isfile(join(path,f))]
        for each in files:
            self.create_image_for_file(each)

    def create_image_for_file(self, file_add):
        print('adding im', file_add)
        self.files_being_tracked[file_add] = im = Image(source=file_add, 
            allow_stretch=True)
        self.add_widget(im)

    def remove_image_for_file(self, file_add):
        print('removing im', file_add)
        files_being_tracked = self.files_being_tracked
        try:
            im = files_being_tracked[file_add]
            self.remove_widget(im)
            del files_being_tracked[file_add]
        except:
            print(file_add, 'not found')

    def add_folder_observer(self, callback, folder_to_observe):
        folder_observers = self.folder_observers
        if folder_to_observe not in folder_observers:
            folder_observers[folder_to_observe] = []
        folder_observers[folder_to_observe].append(callback)

    def remove_folder_observer(self, callback, folder_to_observe):
        folder_observers = self.folder_observers
        folder_observers[folder_to_observe].remove(callback)

    def file_callback(self, file_add, is_deleted, dt):
        print('file callback start', file_add, is_deleted)
        files_being_tracked = self.files_being_tracked
        if not is_deleted:
            if file_add in self.files_being_tracked:
                print('file already here')
                im = files_being_tracked[file_add]
                im.reload()
            else:
                print('creating image')
                self.create_image_for_file(file_add)
        else:
            self.remove_image_for_file(file_add)

    def notify_folder_observers(self, dt):
        folder_observers = self.folder_observers
        for file_add in self.updated_folders:
            if str(file_add) in folder_observers:
                print('true', file_add, 'being observed')
                callbacks = folder_observers[file_add]
                for callback in callbacks:
                    callback(file_add)
                    

class DriveMainThread(Thread):


    def __init__(self, folders_to_track, start_sync_at, pyfile, **kwargs):
        super(DriveMainThread, self).__init__(**kwargs)
        self.drive_data = DBInterface(os.path.dirname(
            os.path.abspath(pyfile))+'/drive_data/', 'drive_files')
        base_path = os.path.dirname(os.path.abspath(pyfile))+'/'
        self.updated_folders = set()
        self.start_sync_at = start_sync_at
        self.folders_to_track = folders_to_track
        self.setup_drive()
        self.update_queue = Queue.Queue()
        self.get_tracked_folder_ids( 
            folders_to_track, os.path.dirname(
            os.path.abspath(pyfile))+'/')
        self.tracked_widgets = {}
        self.folders_to_track = [
            base_path+path+'/' for path in folders_to_track]

    def add_drive_widget(self, widget, folder_to_track):
        assert(folder_to_track in self.folders_to_track)
        tracked_widgets = self.tracked_widgets
        if folder_to_track not in tracked_widgets:
            tracked_widgets[folder_to_track] = [widget]
        else:
            tracked_widgets[folder_to_track].append(widget)

    def setup_drive(self):
        credentials = storage.get()
        self.queue = queue = Queue.Queue()
        if credentials is None:
            flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, 
                redirect_uri=REDIRECT_URI)
            authorize_url = flow.step1_get_authorize_url()
            print 'Go to the following link in your browser: ' + authorize_url
            code = raw_input('Enter verification code: ').strip()
            credentials = flow.step2_exchange(code)
            storage.put(credentials)
        http = httplib2.Http()
        http = credentials.authorize(http)
        self.drive_service = drive_service = build('drive', 'v2', http=http)
        self.thread_pool = thread_pool = DriveThreadPool(2, queue, 
            credentials)

    def get_tracked_folder_ids(self, folders_to_track, address):
        drive_data = self.drive_data
        for folder_name in self.folders_to_track:
            print(folder_name, address)
            q="title = '{}'".format(folder_name)
            data = self.drive_service.children().list(
                folderId='root', q=q).execute()
            folder_add = address+folder_name+'/'
            print(folder_add)
            self.ensure_dir(folder_add)
            #self.add_folder_observer(self.get_callback, unicode(folder_add))
            for dat in data[u'items']:
                item_id = dat[u'id']
                drive_data.set_entry(
                    'folders', item_id, 'file_add', folder_add)
                drive_data.set_entry(
                    'tracked_items', item_id, 'file_add', folder_add)
    

    def check_file_in_tracked_folders(self, file_data):
        parents = file_data[u'parents']
        folders = self.drive_data.get_table('folders')

        for parent in parents:
            parent_id = parent[u'id']
            print(file_data[u'title'], parent_id, folders)
            if parent_id in folders:
                return True
        return False

    def run(self):
        while True:
            query_download = self.update_queue.get()
            if query_download:
                print('we should download')
                self.retrieve_all_changes()
                self.update_queue.task_done()
       

    def download_folder(self, file_id, file_data):
        parents = file_data[u'parents']
        drive_data = self.drive_data
        #save_data = self.save_data
        folders = drive_data.get_table('folders')
        get_entry = drive_data.get_entry
        append_entry = drive_data.append_entry
        for parent in parents:
            parent_id = parent[u'id']
            if parent_id in folders:
                address = get_entry('folders', parent_id, 'file_add')
                folder_name = file_data[u'title']
                folder_add = address+folder_name+'/'
                self.ensure_dir(folder_add)
                append_entry('folders', file_id, 'file_add', folder_add)
                append_entry('tracked_items', file_id, 'file_add', folder_add)

    def place_address_in_queue(self, address, file_name, file_id, 
        time_since_epoch, download_url):
        drive_data = self.drive_data
        self.ensure_dir(address)
        file_add = address+file_name
        drive_data.append_entry('tracked_items', file_id, 'file_add', 
            file_add)
        do_download = False
        try:
            mod_time = os.stat(file_add).st_mtime
            if time_since_epoch > mod_time:
                do_download = True
            else:
                do_download = False
        except:
            do_download = True
        if do_download:
            self.queue.put((self._download, [download_url, file_add, 
                time_since_epoch], {}))

    def download_file(self, file_id, file_data):
        mod_date = file_data[u'modifiedDate']
        parents = file_data[u'parents']
        time_since_epoch = self.calculate_time_since_epoch(mod_date)
        drive_data = self.drive_data
        folders = drive_data.get_table('folders')
        get_entry = drive_data.get_entry
        download_url = file_data[u'downloadUrl']
        file_name = file_data[u'title']
        place_address_in_queue = self.place_address_in_queue
        for parent in parents:
            parent_id = parent[u'id']
            if parent_id in folders:
                address = get_entry('folders', parent_id, 'file_add')
                print('DOWNLOAD ADDRESS HERE', address)
                if isinstance(address, list):
                    for add in address:
                        place_address_in_queue(add, file_name, file_id,
                            time_since_epoch, download_url)
                else:
                    place_address_in_queue(address, file_name, file_id,
                            time_since_epoch, download_url)

    def retrieve_all_changes(self):
        result = []
        page_token = None
        drive_data = self.drive_data
        largest_change = int(self.get_largest_change_id())
        print('largest', largest_change)
        try:
            start_change_id = str(int(
                drive_data.get_entry('changes', 'last_change', 'id')) + 1)
        except:
            start_change_id = str(int(self.start_sync_at))
        # if self.progress_tracker.start_change_id is None:
        #     self.progress_tracker.start_change_id = int(start_change_id)
        print('start changes at', start_change_id)
        # self.progress_tracker.last_change_id = int(start_change_id)
        if largest_change >= int(start_change_id):
            print('we should parse')
            # Clock.schedule_once(partial(self.retrieve_page_of_changes, None, 
            #     start_change_id))
            while True:
                param = {}
                print('grabbing changes')
                if start_change_id is not None:
                    param['startChangeId'] = start_change_id
                if page_token is not None:
                    param['pageToken'] = page_token
                changes = self.drive_service.changes().list(**param).execute()
                self.parse_changes(changes)
                #self.sync()
                page_token = changes.get('nextPageToken')
                if not page_token:
                    print('done breaking')
                    break

    def parse_changes(self, changes):
        drive_data = self.drive_data
        download_folder = self.download_folder
        download_file = self.download_file
        remove_file = self.remove_file
        tracked_items = drive_data.get_table('tracked_items')
        check_file_in_tracked_folders = self.check_file_in_tracked_folders
        get_is_folder = self.get_is_folder
        set_entry = drive_data.set_entry
        get_file = self.get_file
        if len(changes['items']) == 0:
            return None
        for change in changes['items']:
            file_id = change[u'fileId']
            change_id = change[u'id']
            file_data = get_file(file_id)
            is_deleted = change[u'deleted']
            if is_deleted and file_id in tracked_items:
                self.handle_remove_callback(file_id)
                remove_file(file_id)
            if file_data != None:
                if check_file_in_tracked_folders(file_data):
                    if get_is_folder(file_data):
                        download_folder(file_id, file_data)
                    else:
                        download_file(file_id, file_data)
            set_entry('changes', 'last_change', 'id', change_id)
        return change_id


    def handle_remove_callback(self, file_id):
        file_adds = self.drive_data.get_entry(
            'tracked_items', file_id, 'file_add')
        tracked_widgets = self.tracked_widgets
        if file_adds is not None:
            if isinstance(file_adds, list):
                for file_add in file_adds:
                    for wid in tracked_widgets[file_adds]:
                        file_callback = wid.file_callback
                        Clock.schedule_once(partial(file_callback, file_add, 
                            True))

    def remove_file(self, file_id):
        drive_data = self.drive_data
        to_remove = []
        to_remove_a = to_remove.append
        remove_entry = drive_data.remove_entry
        file_adds = drive_data.get_entry('tracked_items', file_id, 'file_add')
        if file_adds is not None:
            for file_add in file_adds:
                try:
                    os.remove(file_add)
                    print('removing', file_add, file_id)
                    to_remove_a(file_add)
                except:
                    continue
            for add in to_remove:
                remove_entry('tracked_items', file_id, 'file_add', add)

    def ensure_dir(self, f):
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d)

    def calculate_time_since_epoch(self, drive_time):
        date, current_time = drive_time.split('T')
        year, month, day = date.split('-')
        hours, minutes, seconds = current_time.split(':')
        seconds = seconds.split('.')[0]
        py_date = datetime.datetime(int(year), int(month), int(day), 
            hour=int(hours), minute=int(minutes), 
            second=int(seconds))
        return time.mktime(py_date.timetuple())

    def get_is_folder(self, file_data):
        return file_data[u'mimeType'] == u'application/vnd.google-apps.folder'

    def get_file(self, fid):
        try:
            return self.drive_service.files().get(fileId=fid).execute()
        except:
            return None

    def list_folder_content(self, fid):
        return self.drive_service.children().list(folderId=fid).execute()

    def get_about_data(self):
        return self.drive_service.about().get().execute()

    def get_largest_change_id(self):
        return self.get_about_data()[u'largestChangeId']
    
    def get_content_title(self, fid):
        return self.drive_service.files().get(fileId=fid).execute()[u'title']

    def _download(self, queue, drive_service, download_url, name, time):
        if download_url:
            resp, content = drive_service._http.request(download_url)
            if resp.status == 200:
                with open(name, 'w') as save_file:
                    save_file.write(content)
                os.utime(name, (os.stat(name).st_atime, 
                        time))
                print('scheduled callback', name)
                folder_name = os.path.dirname(name) + '/'
                tracked_widgets = self.tracked_widgets
                print(tracked_widgets)
                if folder_name in tracked_widgets:
                    for wid in tracked_widgets[folder_name]:
                        Clock.schedule_once(partial(wid.file_callback,
                            name, False), 15.)
                queue.task_done()
            else:
                print 'An error occurred: %s' % resp


class DriveThreadPool(object):

    def __init__(self, thread_count, queue, credentials):
        for x in range(thread_count):
            t = DriveContentThread(queue, credentials)
            t.setDaemon(True)
            t.start()


class DriveContentThread(Thread):

    def __init__(self, queue, credentials):
        super(DriveContentThread, self).__init__()
        self.queue = queue
        http = httplib2.Http()
        http = credentials.authorize(http)
        self.drive_service = drive_service = build('drive', 'v2', http=http)

    def run(self):
        queue = self.queue
        drive_service = self.drive_service
        while True:
            func, args, kwargs = queue.get()
            if func:
                func(queue, drive_service, *args, **kwargs)
                

class TestApp(App):

    def build(self):
        pass

    def on_start(self):
        pass

if __name__ == '__main__':
    TestApp().run()
