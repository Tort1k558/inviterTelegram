import asyncio
import random
import sqlite3
import sys
import time
from datetime import datetime
import json

import pandas as pd
import python_socks

from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtWidgets import *

from telethon.errors import *
from telethon.sync import TelegramClient
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest

df = pd.read_csv('participants.csv')
users_participants = []
for index, row in df.iterrows():
    users_participants.append({'id': int(row['Id']), 'username': str(row['Username'])})

# group_from = await client.get_entity(int('-1001784080760'))
# participants = await client.get_participants(group_from, aggressive=True)
# idsParticipants = []
# usernamesParticipants = []
# for participant in participants:
#    idsParticipants.append(participant.id)
#    usernamesParticipants.append(participant.username)
# df = pd.DataFrame({'Id': idsParticipants, 'Username': usernamesParticipants})
# df.to_csv('participants.csv', index=False)


settings = {}

conn = sqlite3.connect("accounts.sqlite")
cur = conn.cursor()


class Worker:
    def __init__(self, number, api_id, api_hash, proxy_type=None, proxy=None, mute=0):
        self.number = number
        self.api_id = api_id
        self.api_hash = api_hash
        self.proxy_type = proxy_type
        self.proxy = proxy
        self.mute = mute


class SaveDbThread(QThread):
    log_signal = pyqtSignal(str)

    def run(self):
        global df
        while (True):
            try:
                time.sleep(1 * 60)
                df.to_csv('participants.csv', index=False)
            except Exception as e:
                self.log_signal.emit(f'Ошибка записи бд: {e}')


class LoadWorkersThread(QThread):
    run_signal = pyqtSignal(Worker)
    log_signal = pyqtSignal(str)

    def __init__(self, workers):
        super().__init__()
        self.workers = workers

    def run(self):
        for worker in self.workers:
            delay = random.randint(settings['addition_delay_from'], settings['addition_delay_to'])
            self.log_signal.emit(f'{worker.number} запустится через {delay}')
            time.sleep(delay)
            self.run_signal.emit(worker)


class WorkerThread(QThread):
    log_signal = pyqtSignal(str)
    code_signal = pyqtSignal(QThread)

    def __init__(self, worker):
        super().__init__()
        self.worker = worker
        self.code = ''
        self.client = None

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.process_worker())

    async def sleep(self):
        while self.worker.mute > 0:
            await asyncio.sleep(1)
            self.worker.mute = self.worker.mute - 1

    async def authorize(self):

        try:
            if self.worker.proxy is None or self.worker.proxy == "":
                self.client = TelegramClient(self.worker.number, self.worker.api_id, self.worker.api_hash)
            else:
                match = re.search(r'([\d.]+):([\d.]+):(.+):(.+)', self.worker.proxy)
                if match:
                    ip = match[1]
                    port = int(match[2])
                    login = match[3]
                    password = match[4]
                    proxy_type = None
                    if self.worker.proxy_type == 'SOCKS5':
                        proxy_type = python_socks.ProxyType.SOCKS5
                    elif self.worker.proxy_type == 'SOCKS4':
                        proxy_type = python_socks.ProxyType.SOCKS4
                    elif self.worker.proxy_type == 'HTTP':
                        proxy_type = python_socks.ProxyType.HTTP

                    self.client = TelegramClient(self.worker.number, self.worker.api_id, self.worker.api_hash, proxy=
                    {
                        'proxy_type': proxy_type,
                        'addr': ip,
                        'port': port,
                        'username': login,
                        'password': password,
                        'rdns': True
                    })
                else:
                    self.log_signal.emit(f"Ошибка-{self.worker.number}: не удалось распарсить прокси!")
                    return False
            await self.client.connect()
            if not await self.client.is_user_authorized():
                await self.client.send_code_request(self.worker.number)
                self.code_signal.emit(self)
                while self.code == '':
                    pass
                await self.client.sign_in(self.worker.number, self.code)
        except Exception as e:
            self.log_signal.emit(f"Ошибка-{self.worker.number}: {e}")
            await self.client.disconnect()
            return False
        return True

    async def process_worker(self):
        global df

        if await self.authorize():
            self.log_signal.emit(f'{self.worker.number} успешно авторизовался и начинает свою работу!')
        else:
            self.log_signal.emit(f'{self.worker.number} не удалось авторизоваться, завершение работы!')
            return

        group_to = None
        try:
            group_to = await self.client.get_entity(settings['id_group'])
        except Exception as e:
            self.log_signal.emit(str(e))
            self.log_signal.emit(f'{self.worker.number}: не был в группе и присоединится в течении 2-х минут')
            await asyncio.sleep(60)
            await self.client(JoinChannelRequest(channel=settings['username_group']))
            await asyncio.sleep(60)
            group_to = await self.client.get_entity(settings['id_group'])

        count_requests = 0
        count_invited = 0

        await asyncio.sleep(random.randint(60, 120))
        if self.worker.mute > 0:
            self.log_signal.emit(f'{self.worker.number}: до сих пор в бане на {self.worker.mute}')
            await self.sleep()
        while len(users_participants) > 0:
            user = users_participants[0]
            del users_participants[0]

            df = df[df['Username'] != user['username']]
            try:
                if count_invited > settings['max_invites_per_account']:
                    self.log_signal.emit(f'{self.worker.number}: завершил свою работу')
                    self.worker.mute = 60 * 60 * 12
                    await self.sleep()
                    count_invited = 0
                    count_requests = 0
                    self.log_signal.emit(f'{self.worker.number}: начал свою работу')
                if user['username'] == '':
                    continue

                user_to_add = await self.client.get_input_entity(user['username'])
                self.worker.mute = random.randint(settings['delay_get_user_from'], settings['delay_get_user_to'])
                self.log_signal.emit(f'{self.worker.number}: получил информацию о пользователе {user["username"]} '
                                     f'и ушёл в отдых на {self.worker.mute}')
                await self.sleep()

                await self.client(InviteToChannelRequest(group_to, [user_to_add]))
                self.worker.mute = random.randint(settings['delay_request_from'], settings['delay_request_to'])
                self.log_signal.emit(
                    f'{self.worker.number}: пригласил {user["username"]} и ушёл в отдых на {self.worker.mute}')
                await self.sleep()

                count_invited += 1
                count_requests += 1

            except UserPrivacyRestrictedError:
                self.worker.mute = random.randint(settings['delay_request_from'], settings['delay_request_to'])
                self.log_signal.emit(
                    f'{self.worker.number}: Не удалось пригласить {user["username"]} ошибка приватности')
                await self.sleep()
                count_requests = count_requests + 1
                count_invited = count_invited + 1
            except FloodWaitError as e:
                self.worker.mute = e.seconds + random.randint(120, 240)
                self.log_signal.emit(f'{self.worker.number}: Получил временный флуд ушёл в мут на {self.worker.mute}')
                await self.sleep()
                count_requests = count_requests + 1
            except PeerFloodError:
                self.worker.mute = settings['delay_peer_flood']
                self.log_signal.emit(f'{self.worker.number}: Получил флуд запросами и ушёл в мут на {self.worker.mute}')
                await self.sleep()
                count_requests = count_requests + 1
            except ConnectionError:
                self.log_signal.emit(f'{self.worker.number}: Ошибка подключения, пытаюсь заново авторизоваться!')
                await self.authorize()
                count_requests = count_requests + 1
            except Exception as e:
                self.worker.mute = random.randint(settings['delay_request_from'], settings['delay_request_to'])
                self.log_signal.emit(
                    f'{self.worker.number}: Непредвиденная ошибка,  {e} и ушёл в мут на {self.worker.mute}')
                await self.sleep()
                count_requests = count_requests + 1

    def stop(self):
        cur.execute('UPDATE accounts SET mute = ? WHERE number = ?', [self.worker.mute, self.worker.number])
        conn.commit()
        self.client.disconnect()
        self.terminate()


class AccountManager(QWidget):
    def __init__(self):
        super().__init__()
        self.is_running = False
        self.worker_threads = []
        self.load_accounts_thread = None

        self.init_ui()
        self.load_settings()

        self.workers = []
        self.update_workers()
        self.save_db_thread = SaveDbThread()
        self.save_db_thread.log_signal.connect(self.log)
        self.save_db_thread.start()

    def init_ui(self):

        main_layout = QVBoxLayout()

        all_settings_layout = QVBoxLayout()

        settings_layout0 = QHBoxLayout()

        settings_layout0.addWidget(QLabel('Delay request from:'))
        self.delay_request_from_edit = QSpinBox(self)
        self.delay_request_from_edit.setRange(0, 24 * 60 * 60)
        self.delay_request_from_edit.editingFinished.connect(self.save_settings)
        settings_layout0.addWidget(self.delay_request_from_edit)

        settings_layout0.addWidget(QLabel('Delay request to:'))
        self.delay_request_to_edit = QSpinBox(self)
        self.delay_request_to_edit.setRange(0, 24 * 60 * 60)
        self.delay_request_to_edit.editingFinished.connect(self.save_settings)
        settings_layout0.addWidget(self.delay_request_to_edit)

        all_settings_layout.addLayout(settings_layout0)

        settings_layout1 = QHBoxLayout()

        settings_layout1.addWidget(QLabel('Delay get user from:'))
        self.delay_get_user_from_edit = QSpinBox(self)
        self.delay_get_user_from_edit.setRange(0, 24 * 60 * 60)
        self.delay_get_user_from_edit.editingFinished.connect(self.save_settings)
        settings_layout1.addWidget(self.delay_get_user_from_edit)

        settings_layout1.addWidget(QLabel('Delay get user to:'))
        self.delay_get_user_to_edit = QSpinBox(self)
        self.delay_get_user_to_edit.setRange(0, 24 * 60 * 60)
        self.delay_get_user_to_edit.editingFinished.connect(self.save_settings)
        settings_layout1.addWidget(self.delay_get_user_to_edit)

        all_settings_layout.addLayout(settings_layout1)

        settings_layout2 = QHBoxLayout()

        settings_layout2.addWidget(QLabel('Max invites per account:'))
        self.max_invites_per_account_edit = QSpinBox(self)
        self.max_invites_per_account_edit.setRange(0, 24 * 60 * 60)
        self.max_invites_per_account_edit.editingFinished.connect(self.save_settings)
        settings_layout2.addWidget(self.max_invites_per_account_edit)

        all_settings_layout.addLayout(settings_layout2)

        settings_layout3 = QHBoxLayout()

        settings_layout3.addWidget(QLabel('Id group:'))
        self.id_group_edit = QLineEdit(self)
        self.id_group_edit.editingFinished.connect(self.save_settings)
        settings_layout3.addWidget(self.id_group_edit)

        settings_layout3.addWidget(QLabel('Username group:'))
        self.username_group_edit = QLineEdit()
        self.username_group_edit.editingFinished.connect(self.save_settings)
        settings_layout3.addWidget(self.username_group_edit)

        all_settings_layout.addLayout(settings_layout3)

        settings_layout4 = QHBoxLayout()

        settings_layout4.addWidget(QLabel('Addition delay from:'))
        self.addition_delay_from_edit = QSpinBox(self)
        self.addition_delay_from_edit.setRange(0, 24 * 60 * 60)
        self.addition_delay_from_edit.editingFinished.connect(self.save_settings)
        settings_layout4.addWidget(self.addition_delay_from_edit)

        settings_layout4.addWidget(QLabel('Addition delay to:'))
        self.addition_delay_to_edit = QSpinBox(self)
        self.addition_delay_to_edit.setRange(0, 24 * 60 * 60)
        self.addition_delay_to_edit.editingFinished.connect(self.save_settings)
        settings_layout4.addWidget(self.addition_delay_to_edit)

        all_settings_layout.addLayout(settings_layout4)

        settings_layout5 = QHBoxLayout()

        settings_layout5.addWidget(QLabel('Delay peer flood:'))
        self.delay_peer_flood_edit = QSpinBox(self)
        self.delay_peer_flood_edit.setRange(0, 24 * 60 * 60)
        self.delay_peer_flood_edit.editingFinished.connect(self.save_settings)
        settings_layout5.addWidget(self.delay_peer_flood_edit)

        all_settings_layout.addLayout(settings_layout5)

        main_layout.addLayout(all_settings_layout)

        accounts_layout = QVBoxLayout()
        self.workers_list = QListWidget(self)
        accounts_layout.addWidget(self.workers_list)

        input_layout1 = QHBoxLayout()
        input_layout2 = QHBoxLayout()
        input_layout3 = QHBoxLayout()

        input_layout1.addWidget(QLabel("Api_id:"))
        self.id_input = QLineEdit(self)
        input_layout1.addWidget(self.id_input)

        input_layout1.addWidget(QLabel("Api_hash:"))
        self.hash_input = QLineEdit(self)
        input_layout1.addWidget(self.hash_input)

        input_layout2.addWidget(QLabel("Number:"))
        self.number_input = QLineEdit(self)
        input_layout2.addWidget(self.number_input)

        input_layout3.addWidget(QLabel('Proxy type:'))
        self.proxy_type_input = QComboBox(self)
        self.proxy_type_input.addItems(['SOCKS5', 'SOCKS4', 'HTTP'])
        input_layout3.addWidget(self.proxy_type_input)

        input_layout3.addWidget(QLabel('Proxy:'))
        self.proxy_input = QLineEdit(self)
        input_layout3.addWidget(self.proxy_input)

        accounts_layout.addLayout(input_layout2)
        accounts_layout.addLayout(input_layout1)
        accounts_layout.addLayout(input_layout3)

        add_button = QPushButton('Добавить аккаунт', self)
        add_button.clicked.connect(self.add_worker_slot)

        remove_button = QPushButton('Удалить аккаунт', self)
        remove_button.clicked.connect(self.remove_worker_slot)

        edit_button_layout = QHBoxLayout()
        edit_button_layout.addWidget(add_button)
        edit_button_layout.addWidget(remove_button)

        accounts_layout.addLayout(edit_button_layout)

        workers_layout = QVBoxLayout()

        self.running_workers_list = QListWidget(self)
        workers_layout.addWidget(self.running_workers_list)

        self.log_widget = QPlainTextEdit(self)
        self.log_widget.setReadOnly(True)
        workers_layout.addWidget(self.log_widget)
        start_button = QPushButton('Start', self)
        start_button.clicked.connect(self.run_workers)
        stop_button = QPushButton('Stop', self)
        stop_button.clicked.connect(self.stop_workers)

        launch_button_layout = QHBoxLayout()
        launch_button_layout.addWidget(stop_button)
        launch_button_layout.addWidget(start_button)

        workers_layout.addLayout(launch_button_layout)

        work_layout = QHBoxLayout()

        work_layout.addLayout(accounts_layout)
        work_layout.addLayout(workers_layout)

        main_layout.addLayout(work_layout)
        self.setLayout(main_layout)

        self.setGeometry(300, 300, 800, 600)
        self.setWindowTitle('TgInviter')

        self.show()

    def add_worker_slot(self):
        api_id = self.id_input.text()
        api_hash = self.hash_input.text()
        number = self.number_input.text()
        proxy_type = self.proxy_type_input.currentText()
        proxy = self.proxy_input.text()

        cur.execute('SELECT number from accounts WHERE number = ?;',
                    [number])
        numbers = cur.fetchall()
        if len(numbers) != 0:
            self.log('Ошибка: аккаунт с данным номером уже добавлен!')
            return
        if api_id and api_hash and number:
            cur.execute("""INSERT INTO accounts(number,api_id,api_hash,proxy_type,proxy,mute) 
                        VALUES(?,?,?,?,?,?)""", [number, int(api_id), api_hash, proxy_type, proxy, 0])
            conn.commit()

            if self.is_running:
                worker = Worker(number, api_id, api_hash, proxy_type, proxy)
                self.run_worker(worker)

            self.id_input.clear()
            self.hash_input.clear()
            self.number_input.clear()
            self.proxy_input.clear()

            self.update_workers()

    def remove_worker_slot(self):
        selected_item = self.workers_list.currentItem()
        if not selected_item:
            return

        match_number = re.search(r'Number: ([\d+]+);', selected_item.text())
        if not match_number:
            return

        number = str(match_number.group(1))

        cur.execute(f'DELETE FROM accounts WHERE number = ? ;', (number,))
        conn.commit()

        for worker_thread in self.worker_threads:
            if worker_thread.worker.number == number:
                worker_thread.stop()
                self.worker_threads.remove(worker_thread)

        self.update_running_workers()
        self.update_workers()

    def log(self, msg):
        self.log_widget.appendPlainText(f'{datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")}:{msg}')

    def get_code(self, thread):
        thread.code, ok = QInputDialog.getText(self, f'Введите код', thread.worker.number)
        while ok is False or thread.code == '':
            thread.code, ok = QInputDialog.getText(self, f'Введите код', thread.worker.number)

    def stop_workers(self):
        if self.is_running:
            self.log('Stopping workers')

            self.load_accounts_thread.terminate()
            for worker_thread in self.worker_threads:
                worker_thread.stop()
            self.worker_threads.clear()

            self.update_running_workers()
            self.is_running = False

    def run_workers(self):
        if not self.is_running:
            self.log('Running workers')

            self.load_accounts_thread = LoadWorkersThread(self.workers)
            self.load_accounts_thread.run_signal.connect(self.run_worker)
            self.load_accounts_thread.log_signal.connect(self.log)
            self.load_accounts_thread.start()

            self.is_running = True

    def run_worker(self, worker: Worker):
        worker_thread = WorkerThread(worker)
        worker_thread.log_signal.connect(self.log)
        worker_thread.code_signal.connect(self.get_code)
        worker_thread.start()
        self.worker_threads.append(worker_thread)
        self.update_running_workers()

    def update_workers(self):
        cur.execute("""CREATE TABLE IF NOT EXISTS accounts(
                   number TEXT PRIMARY KEY,
                   api_id INT,
                   api_hash TEXT,
                   proxy_type TEXT,
                   proxy TEXT,
                   mute INT
                   );
                """)
        conn.commit()

        cur.execute("SELECT * FROM accounts;")

        self.workers_list.clear()
        workers = cur.fetchall()
        self.workers.clear()

        for worker in workers:
            worker = Worker(worker[0], worker[1], worker[2], worker[3], worker[4], worker[5])
            self.workers.append(worker)
            self.workers_list.addItem(f'Number: {worker.number}; Api_id: {worker.api_id}; Api_hash: {worker.api_hash};'
                                      f'Proxy_type: {worker.proxy_type}; Proxy: {worker.proxy}; ')

    def update_running_workers(self):
        self.running_workers_list.clear()
        for worker_thread in self.worker_threads:
            self.running_workers_list.addItem(
                f'Number: {worker_thread.worker.number}; Api_id: {worker_thread.worker.api_id}; '
                f'Api_hash: {worker_thread.worker.api_hash}; '
                f'Proxy_type: {worker_thread.worker.proxy_type}; Proxy: {worker_thread.worker.proxy}; ')

    def closeEvent(self, event):
        reply = QMessageBox.question(self, 'Подтверждение', 'Вы уверены, что хотите закрыть приложение?',
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)

        if reply == QMessageBox.StandardButton.Yes:
            self.stop_workers()
            time.sleep(3)
            event.accept()
        else:
            event.ignore()

    def load_settings(self):
        global settings
        try:
            with open('settings.json', 'r') as file:
                settings = json.load(file)
        except FileNotFoundError:
            settings = {'delay_request_from': 35 * 60,
                        'delay_request_to': 45 * 60,
                        'delay_get_user_from': 35 * 60,
                        'delay_get_user_to': 45 * 60,
                        'max_invites_per_account': 50,
                        'id_group': '',
                        'username_group': '',
                        'addition_delay_from': 20,
                        'addition_delay_to': 40,
                        'delay_peer_flood': 60 * 60 * 12
                        }

        self.delay_request_from_edit.setValue(settings.get('delay_request_from', 0))
        self.delay_request_to_edit.setValue(settings.get('delay_request_to', 0))

        self.delay_get_user_from_edit.setValue(settings.get('delay_get_user_from', 0))
        self.delay_get_user_to_edit.setValue(settings.get('delay_get_user_to', 0))

        self.max_invites_per_account_edit.setValue(settings.get('max_invites_per_account', 0))
        self.id_group_edit.setText(settings.get('id_group', ''))

        self.username_group_edit.setText(settings.get('username_group', ''))

        self.addition_delay_from_edit.setValue(settings.get('addition_delay_from', 0))
        self.addition_delay_to_edit.setValue(settings.get('addition_delay_to', 0))

        self.delay_peer_flood_edit.setValue(settings.get('delay_peer_flood', 0))

    def save_settings(self):
        settings['delay_request_from'] = self.delay_request_from_edit.value()
        settings['delay_request_to'] = self.delay_request_to_edit.value()
        settings['delay_get_user_from'] = self.delay_get_user_from_edit.value()
        settings['delay_get_user_to'] = self.delay_get_user_to_edit.value()
        settings['max_invites_per_account'] = self.max_invites_per_account_edit.value()
        settings['id_group'] = self.id_group_edit.text()
        settings['username_group'] = self.username_group_edit.text()
        settings['addition_delay_from'] = self.addition_delay_from_edit.value()
        settings['addition_delay_to'] = self.addition_delay_to_edit.value()
        settings['delay_peer_flood'] = self.delay_peer_flood_edit.value()

        try:
            with open('settings.json', 'w') as file:
                json.dump(settings, file)
        except Exception as e:
            self.log(f'Не удалось сохранить настройки {e}!')


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = AccountManager()
    result = app.exec()
    cur.close()
    conn.close()
    sys.exit(result)
