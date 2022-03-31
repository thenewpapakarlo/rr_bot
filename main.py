
# TODO 1. переписать запрос получения данных по дислокации для массива партнёров, чтобы запрос быд всегда один, а не по
# TODO их количеству

from ast import literal_eval
from conf import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
from os import path, remove
from queries import *
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, \
    InlineKeyboardButton
from zeep import Client

import configparser
import json
import logging
import pandas as pd
import pyodbc
import telebot
import xmltodict


def create_logger():
    # logger = logging.getLogger('MainLogger')
    logger = telebot.logger
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename='bot.log', encoding='utf8')
    logger.addHandler(handler)
    formatter = logging.Formatter(fmt='{asctime} [{levelname}]: {message}', style='{')
    handler.setFormatter(formatter)
    return logger


logger = create_logger()

logger.debug('Reading <config.ini>')
config = configparser.ConfigParser()
config.read('config.ini')

price_requests_data = dict()  # хранит текущие данные для передачи на расчёт в ЭТП ГП. Ключ - id чата
cargos = dict()  # хранит наименования типов грузов. Ключ - код груза
packages = dict()  # хранит наименования типов упаковки. Ключ - код типа упаковки
stations = dict()  # хранит наименования и дороги станций. Ключ - код станции
wagons = dict()  # хранит наименования типов вагонов. Ключ - код типа вагона

bot = telebot.TeleBot(config['bot']['token'])


@bot.message_handler(commands=['start'])
def welcome(message):
    bot.send_message(message.chat.id, 'Добро пожаловать в телеграм-бот компании "Еврологистик"!\n')
    user_id = str(message.from_user.id)
    if config.has_section(user_id):
        # Пользователь есть в базе, запрашиваем команду дальнейших действий
        reply_choose_command(message.chat.id)
    else:
        # Пользователя нет в базе бота, запрашиваем контактные данные
        reply_ask_phone(message.chat.id)


@bot.message_handler(commands=['dislocation'])
def dislocation(message):
    user_id = str(message.from_user.id)
    df = get_partner_by_phone(config[user_id]['phone_number'])
    if len(df) == 1:
        # Партнёр один, сразу формируем и отправляем отчёт
        bot.send_message(message.chat.id, 'Ожидайте, идёт запрос к базе данных...')
        df = get_report_data(df.at[0, 'ref'])
        if len(df) > 0:
            report = create_report_file(df)
            bot.send_document(message.chat.id, data=open(report, 'rb'))
            remove(report)
        else:
            bot.send_message(message.chat.id, 'На текущий момент нет данных для формирования отчёта. '
                                              'Попробуйте запросить позднее.')
    else:
        # Партнёров несколько, выводим InlineKeyboard для выбора партнёра, по которому нужно сформировать отчёт
        message_text = 'Выберите фирму:'
        keyboard = get_inline_keyboard(df, mode='partner')
        bot.send_message(message.chat.id, message_text, reply_markup=keyboard)


@bot.message_handler(commands=['calculateprice'])
def calculate_price(message):
    test_calc(message)
    # keyboard = get_reply_keyboard(KEYBOARD_TYPES_YES_NO)
    # new_message = bot.send_message(message.chat.id, 'Необходимо ввести данные для выполнения расчёта. Продолжить?',
    #                                reply_markup=keyboard)
    # bot.register_next_step_handler(new_message, reply_start_calculate)


@bot.message_handler(content_types=['contact'])
def contact(message):
    if message.contact is not None:
        user_id = str(message.contact.user_id)
        if not config.has_section(user_id):
            config.add_section(user_id)
        config.set(user_id, 'phone_number', message.contact.phone_number)
        with open('config.ini', 'w', encoding='utf8') as config_file:
            config.write(config_file)
        reply_partner_name(message.chat.id, message.contact.phone_number)


@bot.callback_query_handler(func=lambda call: json.loads(call.data)['type'] == 'partner')
def callback_inline_partner(call):
    bot.edit_message_reply_markup(call.message.chat.id, call.message.id, reply_markup=None)
    data = json.loads(call.data)
    if data['ref'] == 'all':
        user_id = str(call.from_user.id)
        df = get_partner_by_phone(config[user_id]['phone_number'])
        data = pd.DataFrame()  # columns=('Partner_Name',))
        for _, row in df.iterrows():
            to_append = get_report_data(row['ref'])
            data = data.append(to_append, ignore_index=True)
    else:
        ref = bytes.fromhex(data['ref'])
        data = get_report_data(ref)
    bot.send_message(call.message.chat.id, 'Ожидайте, идёт запрос к базе данных...')
    reply_report_data(call.message.chat.id, data)


@bot.callback_query_handler(func=lambda call: json.loads(call.data)['type'] == 'cargo')
def callback_inline_cargo(call):
    bot.edit_message_reply_markup(call.message.chat.id, call.message.id, reply_markup=None)
    data = json.loads(call.data)
    bot.send_message(call.message.chat.id, f'{data["code"]} - {cargos[data["code"]]}', reply_markup=None)
    price_requests_data[call.message.chat.id]['cargo_type_code'] = data['code']
    reply_ask_weight(call.message.chat.id)


@bot.callback_query_handler(func=lambda call: json.loads(call.data)['type'] == 'package')
def callback_inline_package(call):
    bot.edit_message_reply_markup(call.message.chat.id, call.message.id, reply_markup=None)
    data = json.loads(call.data)
    bot.send_message(call.message.chat.id, f'{data["code"]} - {packages[data["code"]]}', reply_markup=None)
    price_requests_data[call.message.chat.id]['cargo_package_type_code'] = data['code']
    reply_ask_start_date(call.message.chat.id)


@bot.callback_query_handler(func=lambda call: json.loads(call.data)['type'] == 'station')
def callback_inline_station(call):
    bot.edit_message_reply_markup(call.message.chat.id, call.message.id, reply_markup=None)
    data = json.loads(call.data)
    bot.send_message(call.message.chat.id,
                     f'{data["code"]} - {stations[data["code"]]["name"]} ({stations[data["code"]]["road_name"]})')
    if 'departure_station_code' not in price_requests_data[call.message.chat.id].keys():
        price_requests_data[call.message.chat.id]['departure_station_code'] = data['code']
        reply_ask_station(call.message.chat.id,
                          'Введите код или наименование станции назначения (целиком или полностью)')
    else:
        price_requests_data[call.message.chat.id]['destination_station_code'] = data['code']
        reply_ask_wagon(call.message.chat.id)


@bot.callback_query_handler(func=lambda call: json.loads(call.data)['type'] == 'wagon')
def callback_inline_package(call):
    bot.edit_message_reply_markup(call.message.chat.id, call.message.id, reply_markup=None)
    data = json.loads(call.data)
    bot.send_message(call.message.chat.id, f'{data["code"]} - {wagons[data["code"]]}', reply_markup=None)
    price_requests_data[call.message.chat.id]['wagon_type_code'] = data['code']
    reply_ask_confirm_calculation_parameters(call.message.chat.id)


def reply_start_calculate(message):
    if message.text.lower() == 'да':
        price_requests_data[message.chat.id] = dict()
        reply_ask_cargo(message.chat.id)
    else:
        reply_choose_command(message.chat.id)


def reply_ask_cargo(chat_id, message_text='Введите код груза или его наименование (целиком или полностью)'):
    new_message = bot.send_message(chat_id, message_text, reply_markup=ReplyKeyboardRemove())
    # get_reply_keyboard())
    bot.register_next_step_handler(new_message, reply_select_cargo)


def reply_select_cargo(message):
    df = get_cargo_type_code(message.text)
    store_data(cargos, df)
    if len(df) == 0:
        reply_ask_cargo(message.chat.id, 'Груз с таким кодом/наименованием не найден. Пожалуйста, повторите ввод.')
    elif len(df) == 1:
        new_message = bot.send_message(message.chat.id,
                                       f'Вы хотите отправить груз "{df.at[0, "Name"]}" с кодом {df.at[0, "Code"]}?',
                                       reply_markup=get_reply_keyboard(KEYBOARD_TYPES_YES_NO))
        bot.register_next_step_handler(new_message, reply_confirm_cargo, df.at[0, 'Code'])
    elif len(df) > 20:
        reply_ask_cargo(message.chat.id, 'Найдено слишком много вариантов. Пожалуйста, повторите ввод.')
    else:
        keyboard = get_inline_keyboard(df, mode='cargo')
        bot.send_message(message.chat.id, 'Выберите груз:', reply_markup=keyboard)


def reply_confirm_cargo(message, cargo_type_code):
    if message.text.lower() == 'да':
        price_requests_data[message.chat.id]['cargo_type_code'] = cargo_type_code
        reply_ask_weight(message.chat.id)
    else:
        reply_ask_cargo(message.chat.id)


def reply_ask_weight(chat_id, message_text='Введите вес груза в кг (целое число)'):
    new_message = bot.send_message(chat_id, message_text)
    bot.register_next_step_handler(new_message, verify_weight)


def verify_weight(message):
    try:
        weight = int(message.text)
        price_requests_data[message.chat.id]['cargo_weight'] = weight
    except ValueError:
        reply_ask_weight(message.chat.id, 'Число указано неверно. Пожалуйста, повторите ввод')
    reply_ask_volume(message.chat.id)


def reply_ask_volume(chat_id, message_text='Введите объём груза в м³ (целое число)'):
    new_message = bot.send_message(chat_id, message_text)
    bot.register_next_step_handler(new_message, verify_volume)


def verify_volume(message):
    try:
        volume = int(message.text)
        price_requests_data[message.chat.id]['cargo_volume'] = volume
    except ValueError:
        reply_ask_weight(message.chat.id, 'Число указано неверно. Пожалуйста, повторите ввод')
    reply_ask_package(message.chat.id)


def reply_ask_package(chat_id, message_text='Введите код типа упаковки или его наименование (целиком или полностью)'):
    new_message = bot.send_message(chat_id,
                                   message_text,
                                   reply_markup=get_reply_keyboard())
    bot.register_next_step_handler(new_message, reply_select_package)


def reply_select_package(message):
    df = get_package_type_code(message.text)
    store_data(packages, df)
    if len(df) == 0:
        reply_ask_package(message.chat.id,
                          'Тип упаковки с таким кодом/наименованием не найден. Пожалуйста, повторите ввод.')
    elif len(df) == 1:
        new_message = bot.send_message(message.chat.id,
                                       f'Найден тип упаковки "{df.at[0, "Name"]}" с кодом {df.at[0, "Code"]}. Верно?',
                                       reply_markup=get_reply_keyboard(KEYBOARD_TYPES_YES_NO))
        bot.register_next_step_handler(new_message, reply_confirm_package, df.at[0, 'Code'])
    elif len(df) > 20:
        reply_ask_package(message.chat.id, 'Найдено слишком много вариантов. Пожалуйста, повторите ввод.')
    else:
        keyboard = get_inline_keyboard(df, mode='package')
        bot.send_message(message.chat.id, 'Выберите тип упаковки:', reply_markup=keyboard)


def reply_confirm_package(message, package_type_code):
    if message.text.lower() == 'да':
        price_requests_data[message.chat.id]['cargo_package_type_code'] = package_type_code
        reply_ask_start_date(message.chat.id)
    else:
        reply_ask_package(message.chat.id)


def reply_ask_start_date(chat_id, message_text="Введите дату отправки груза в формате 'дд.мм.гггг'"):
    new_message = bot.send_message(chat_id, message_text, reply_markup=ReplyKeyboardRemove())
    bot.register_next_step_handler(new_message, verify_date)


def verify_date(message):
    start_date = message.text
    try:
        date_parts = [int(date_part) for date_part in start_date.split('.')]
        if len(date_parts) != 3:
            raise ValueError
        # Проверяем, существует ли такая дата
        datetime(date_parts[2], date_parts[1], date_parts[0])
    except ValueError:
        reply_ask_start_date(message.chat.id, 'Дата указана неверно. Пожалуйста, повторите ввод')
    else:
        # Проверки пройдены
        price_requests_data[message.chat.id]['start_date'] = start_date
        price_requests_data[message.chat.id]['finish_date'] = start_date
        reply_ask_station(message.chat.id)


def reply_ask_station(chat_id, message_text='Введите код или наименование станции отправления (целиком или полностью)'):
    new_message = bot.send_message(chat_id, message_text, reply_markup=get_reply_keyboard())
    bot.register_next_step_handler(new_message, reply_select_station)


def reply_select_station(message):
    df = get_station_code(message.text)
    store_stations_data(stations, df)
    if len(df) == 0:
        reply_ask_station(message.chat.id,
                          'Станция с таким кодом/наименованием не найден. Пожалуйста, повторите ввод.')
    elif len(df) == 1:
        new_message = bot.send_message(message.chat.id,
                                       f'Найдена станция "{df.at[0, "Name"]} ({df.at[0, "RW_Name"]})" '
                                       f'с кодом {df.at[0, "Code"]}. Верно?',
                                       reply_markup=get_reply_keyboard(KEYBOARD_TYPES_YES_NO))
        bot.register_next_step_handler(new_message, reply_confirm_station, df.at[0, 'Code'])
    elif len(df) > 20:
        reply_ask_station(message.chat.id, 'Найдено слишком много вариантов. Пожалуйста, повторите ввод.')
    else:
        keyboard = get_inline_keyboard(df, mode='station')
        bot.send_message(message.chat.id, 'Выберите станцию:', reply_markup=keyboard)


def reply_confirm_station(message, station_code):
    if message.text.lower() == 'да':
        if 'departure_station_code' not in price_requests_data[message.chat.id].keys():
            price_requests_data[message.chat.id]['departure_station_code'] = station_code
            reply_ask_station(message.chat.id,
                              'Введите код или наименование станции назначения (целиком или полностью)')
        else:
            price_requests_data[message.chat.id]['destination_station_code'] = station_code
            reply_ask_wagon(message.chat.id)
    else:
        if 'departure_station_code' not in price_requests_data[message.chat.id].keys():
            reply_ask_station(message.chat.id)
        else:
            reply_ask_station(message.chat.id,
                              'Введите код или наименование станции назначения (целиком или полностью)')


def reply_ask_wagon(chat_id, message_text='Введите код или наименование типа вагона (целиком или полностью)'):
    new_message = bot.send_message(chat_id, message_text, reply_markup=ReplyKeyboardRemove())
    bot.register_next_step_handler(new_message, reply_select_wagon)


def reply_select_wagon(message):
    df = get_wagon_type_code(message.text)
    store_data(wagons, df)
    if len(df) == 0:
        reply_ask_wagon(message.chat.id,
                        'Тип вагона с таким кодом/наименованием не найден. Пожалуйста, повторите ввод.')
    elif len(df) == 1:
        new_message = bot.send_message(message.chat.id,
                                       f'Найден тип вагона "{df.at[0, "Name"]}" с кодом {df.at[0, "Code"]}. Верно?',
                                       reply_markup=get_reply_keyboard(KEYBOARD_TYPES_YES_NO))
        bot.register_next_step_handler(new_message, reply_confirm_wagon, df.at[0, 'Code'])
    elif len(df) > 20:
        reply_ask_wagon(message.chat.id, 'Найдено слишком много вариантов. Пожалуйста, повторите ввод.')
    else:
        keyboard = get_inline_keyboard(df, mode='wagon')
        bot.send_message(message.chat.id, 'Выберите тип вагона:', reply_markup=keyboard)


def reply_confirm_wagon(message, wagon_type_code):
    if message.text.lower() == 'да':
        price_requests_data[message.chat.id]['wagon_type_code'] = wagon_type_code
        reply_ask_confirm_calculation_parameters(message.chat.id)
    else:
        reply_ask_wagon(message.chat.id)


def reply_ask_confirm_calculation_parameters(chat_id):
    message_text = 'Пожалуйста, подтвердите параметры для расчёта:\n' + \
                   f'груз - {price_requests_data[chat_id]["cargo_type_code"]} - ' + \
                   f'{cargos[price_requests_data[chat_id]["cargo_type_code"]]};\n' + \
                   f'вес - {price_requests_data[chat_id]["cargo_weight"]} кг;\n' + \
                   f'объём - {price_requests_data[chat_id]["cargo_volume"]} м³;\n' + \
                   f'упаковка - {price_requests_data[chat_id]["cargo_package_type_code"]} - ' + \
                   f'{packages[price_requests_data[chat_id]["cargo_package_type_code"]]};\n' + \
                   f'дата отправки - {price_requests_data[chat_id]["start_date"]};\n' + \
                   f'станция отправления - {price_requests_data[chat_id]["departure_station_code"]} - ' + \
                   f'{stations[price_requests_data[chat_id]["departure_station_code"]]["name"]} (' + \
                   f'{stations[price_requests_data[chat_id]["departure_station_code"]]["road_name"]});\n' + \
                   f'станция назначения - {price_requests_data[chat_id]["destination_station_code"]} - ' + \
                   f'{stations[price_requests_data[chat_id]["destination_station_code"]]["name"]} (' + \
                   f'{stations[price_requests_data[chat_id]["destination_station_code"]]["road_name"]});\n' + \
                   f'тип вагона - {price_requests_data[chat_id]["wagon_type_code"]} - ' + \
                   f'{wagons[price_requests_data[chat_id]["wagon_type_code"]]}'
    new_message = bot.send_message(chat_id, message_text, reply_markup=get_reply_keyboard(KEYBOARD_TYPES_YES_NO))
    bot.register_next_step_handler(new_message, reply_confirm_calculation_parameters)


def reply_confirm_calculation_parameters(message):
    if message.text.lower() == 'да':
        bot.send_message(message.chat.id, 'Ожидайте ответ...', reply_markup=ReplyKeyboardRemove())
        request = get_calculate_price_request_xml(price_requests_data[message.chat.id])
        # print(request)
        if config['common']['test'] == '1':
            login = config['etpgp']['login_test']
            password = config['etpgp']['password_test']
            # wsdl = config['etpgp']['wsdl_test']
            wsdl = 'test.wsdl'
        else:
            login = config['etpgp']['login']
            password = config['etpgp']['password']
            wsdl = config['etpgp']['wsdl']

        client = Client(wsdl)
        # # client.transport.session.proxies = {
        # #     # Utilize for certain URL
        # #     'http://etptest.intellex.ru': 'http://tempuri.org/',
        # # }
        # request_data = {
        #     'cargo_type_code': '595016',
        #     'cargo_weight': '68000',
        #     'cargo_volume': '150',
        #     'cargo_package_type_code': '16',
        #     'start_date': '05.08.2021',
        #     'finish_date': '05.08.2021',
        #     'departure_station_code': '531003',
        #     'destination_station_code': '180909',
        #     'wagon_type_code': '40',
        # }
        # try:
        #     with client.settings(raw_response=False):
        #         result = client.service.GetBlock(login, password, request)
        #         assert result.status_code == 200
        #         assert result.content
        # except AssertionError:
        #     answer = {'result': 'assertion error'}
        # except ConnectionError:
        #     answer = {'result': 'connection error'}
        # else:
        #     answer = xmltodict.parse(result.text)
        try:
            result = client.service.GetBlock(login, password, request)
            answer = xmltodict.parse(result.text)
        except:
            answer = 'error while request "etp gp"'
        # # result = client.service.GetBlock(login, password, '<test version="1.0"></test>')
        # print(answer)
        bot.send_message(message.chat.id, answer, reply_markup=ReplyKeyboardRemove())
        reply_choose_command(message.chat.id)
    else:
        reply_choose_command(message.chat.id)


def test_calc(message):
    bot.send_message(message.chat.id, 'Ожидайте ответ...', reply_markup=ReplyKeyboardRemove())
    with open('etpgp.xml', 'r') as xml:
        request_xml = xml.read()
    if config['common']['test'] == '1':
        login = config['etpgp']['login_test']
        password = config['etpgp']['password_test']
        # wsdl = config['etpgp']['wsdl_test']
        wsdl = 'test.wsdl'
    else:
        login = config['etpgp']['login']
        password = config['etpgp']['password']
        wsdl = config['etpgp']['wsdl']

    client = Client(wsdl)
        # # client.transport.session.proxies = {
        # #     # Utilize for certain URL
        # #     'http://etptest.intellex.ru': 'http://tempuri.org/',
        # # }
    request_data = {
        'cargo_type_code': '595016',
        'cargo_weight': '68000',
        'cargo_volume': '150',
        'cargo_package_type_code': '16',
        'start_date': '20.08.2021',
        'finish_date': '20.08.2021',
        'departure_station_code': '531003',
        'destination_station_code': '180909',
        'wagon_type_code': '40',
    }
    request_xml = request_xml.replace('%cargo_type_code%', request_data['cargo_type_code'])
    request_xml = request_xml.replace('%cargo_weight%', request_data['cargo_weight'])
    request_xml = request_xml.replace('%cargo_volume%', request_data['cargo_volume'])
    request_xml = request_xml.replace('%cargo_package_type_code%', request_data['cargo_package_type_code'])
    request_xml = request_xml.replace('%start_date%', request_data['start_date'])
    request_xml = request_xml.replace('%finish_date%', request_data['finish_date'])
    request_xml = request_xml.replace('%departure_station_code%', request_data['departure_station_code'])
    request_xml = request_xml.replace('%destination_station_code%', request_data['destination_station_code'])
    request_xml = request_xml.replace('%wagon_type_code%', request_data['wagon_type_code'])
    # try:
        #     with client.settings(raw_response=False):
        #         result = client.service.GetBlock(login, password, request)
        #         assert result.status_code == 200
        #         assert result.content
        # except AssertionError:
        #     answer = {'result': 'assertion error'}
        # except ConnectionError:
        #     answer = {'result': 'connection error'}
        # else:
        #     answer = xmltodict.parse(result.text)
    try:
        # result = client.service.GetBlock(login, password, '<test version="1.0"></test>')
        result = client.service.GetBlock(login, password, request_xml)
        # result = client.service.GetBlock(login, password, request)
        answer = result.text
        print(xmltodict.parse(result.text))
    except:
        answer = 'error while request "etp gp"'
    # print(literal_eval(result.text))
    # print(answer)
    bot.send_message(message.chat.id, answer, reply_markup=ReplyKeyboardRemove())
    reply_choose_command(message.chat.id)


def reply_choose_command(chat_id):
    bot.send_message(chat_id, 'Выберите команду из меню', reply_markup=ReplyKeyboardRemove())


def reply_report_data(chat_id, df):
    if len(df) > 0:
        report = create_report_file(df)
        bot.send_document(chat_id, data=open(report, 'rb'))
        remove(report)
    else:
        bot.send_message(chat_id, 'На текущий момент нет данных для формирования отчёта. '
                                  'Попробуйте запросить позднее.')
    reply_choose_command(chat_id)


def reply_ask_phone(chat_id):
    keyboard = get_reply_keyboard(keyboard_type=KEYBOARD_TYPES_CONTACT)
    message_text = 'Для авторизации необходимо сообщить боту свой номер телефона'
    bot.send_message(chat_id, message_text, reply_markup=keyboard)


def reply_partner_name(chat_id, phone_number=None):
    df = get_partner_by_phone(phone_number)
    if len(df) > 0:
        # Партнёр найден
        if len(df) == 1:
            # Партнёр один
            message_text = f'По нашим данным Вы являетесь контактным лицом фирмы {df.at[0, "full_name"]}.\n'
        else:
            # Партнёров больше одного
            partners = ''
            for row in range(len(df)):
                partners += df.at[row, 'full_name'] + ',\n'
            message_text = f'По нашим данным Вы являетесь контактным лицом следующих фирм:\n' \
                           f'{partners[:-2]}.\n'
        bot.send_message(chat_id, message_text)
        reply_choose_command(chat_id)
    else:
        # Партнёр не найден
        keyboard = get_reply_keyboard(keyboard_type=KEYBOARD_TYPES_CONTACT)
        message_text = 'По Вашему номеру телефона не найдено контактное лицо контрагента.\n' \
                       'Попробуйте отправить его ещё раз'
        bot.send_message(chat_id, message_text, reply_markup=keyboard)


def get_partner_by_phone(phone_number):
    # phone_number = '+79100815024'  # DEBUG
    phone_number = '79185234981'  # DEBUG
    return query_database(PARTNER_BY_PHONE_QUERY, (phone_number.replace('+', ''),))


def get_report_data(ref_partner):
    now = (datetime.now() + relativedelta(years=+2000)).strftime('%Y-%m-%d %H:%M:%S')
    return query_database(REPORT_QUERY, (now, now, ref_partner))


def get_cargo_type_code(string):
    return query_database(CARGO_CODE_BY_SUBSTRING_QUERY, ('%' + string + '%',))


def get_package_type_code(string):
    df = query_database(PACKAGE_TYPE_CODE_BY_SUBSTRING_QUERY)
    df['Code'] = [str(int(row['NumCode'])) for _, row in df.iterrows()]
    # TODO возвращает с индексами как в исходном датафрейм
    return df[df['Code'].str.contains(string, case=False) | df['Name'].str.contains(string, case=False)].reset_index()


def get_station_code(string):
    return query_database(STATION_CODE_BY_SUBSTRING_QUERY, ('%' + string + '%',))


def get_wagon_type_code(string):
    df = query_database(WAGON_TYPE_QUERY)
    df['Code'] = [str(int(row['NumCode'])) for _, row in df.iterrows()]
    # TODO возвращает с индексами как в исходном датафрейм
    return df[df['Code'].str.contains(string, case=False) | df['Name'].str.contains(string, case=False)].reset_index()


def query_database(query_text, params=None):
    conn = get_database_connection()
    df = pd.read_sql_query(
        query_text,
        con=conn,
        params=params
    )
    conn.close()
    return df


def create_report_file(df):
    import xlsxwriter
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    temp_dir = config['paths']['temp_dir']
    filename = path.join(temp_dir, f'Dislocation_on_{now}.xlsx')
    workbook = xlsxwriter.Workbook(filename=filename)
    worksheet = workbook.add_worksheet('Дислокация')
    # Стили форматирования ячеек
    header_format = workbook.add_format(HEADER_FORMAT)
    partner_format = workbook.add_format(PARTNER_FORMAT)
    feature_format = workbook.add_format(FEATURE_FORMAT)
    wagon_format = workbook.add_format(WAGON_FORMAT)
    main_text_format = workbook.add_format(MAIN_TEXT_FORMAT)
    main_digit_0_format = workbook.add_format(MAIN_DIGIT_0_FORMAT)
    main_digit_1_format = workbook.add_format(MAIN_DIGIT_1_FORMAT)
    main_digit_2_format = workbook.add_format(MAIN_DIGIT_2_FORMAT)
    # Ширины колонок
    multiplier = 3.917
    worksheet.set_column(0, 0, multiplier * 1.94)
    worksheet.set_column(1, 1, multiplier * 4.95)
    worksheet.set_column(2, 2, multiplier * 3.69)
    worksheet.set_column(3, 3, multiplier * 11.72)
    worksheet.set_column(4, 4, multiplier * 4.95)
    worksheet.set_column(5, 5, multiplier * 4.58)
    worksheet.set_column(6, 6, multiplier * 3.85)
    worksheet.set_column(7, 7, multiplier * 2.15)
    worksheet.set_column(8, 8, multiplier * 1.49)
    worksheet.set_column(9, 9, multiplier * 3.11)
    worksheet.set_column(10, 10, multiplier * 1.86)
    worksheet.set_column(11, 11, multiplier * 4.95)
    worksheet.set_column(12, 12, multiplier * 4.58)
    worksheet.set_column(13, 13, multiplier * 4.95)
    worksheet.set_column(14, 14, multiplier * 4.58)
    worksheet.set_column(15, 15, multiplier * 2.51)
    worksheet.set_column(16, 16, multiplier * 11.72)
    worksheet.set_column(17, 17, multiplier * 1.47)
    worksheet.set_column(18, 18, multiplier * 3.85)
    worksheet.set_column(19, 19, multiplier * 3.85)
    worksheet.set_column(20, 30, multiplier * 3.53)
    worksheet.set_row(3, 25)
    # Шапка
    worksheet.merge_range(0, 0, 0, 20, 'Адресат', header_format)
    worksheet.merge_range(1, 0, 1, 20, 'Признак', header_format)
    worksheet.merge_range(2, 0, 3, 0, 'Вагон', header_format)
    worksheet.merge_range(2, 1, 3, 1, 'Станция подачи', header_format)
    worksheet.merge_range(2, 2, 3, 2, 'Грузоподъёмность (т.)', header_format)
    worksheet.merge_range(2, 3, 3, 3, 'Собственник', header_format)
    worksheet.merge_range(2, 4, 2, 10, 'Операция', header_format)
    worksheet.merge_range(2, 11, 2, 12, 'Отправление', header_format)
    worksheet.merge_range(2, 13, 2, 14, 'Назначение', header_format)
    worksheet.merge_range(2, 15, 3, 15, 'Оставшееся расстояние', header_format)
    worksheet.merge_range(2, 16, 3, 16, 'Груз', header_format)
    worksheet.merge_range(2, 17, 3, 17, 'Вес', header_format)
    worksheet.merge_range(2, 18, 3, 18, 'Дата погрузки', header_format)
    worksheet.merge_range(2, 19, 3, 19, 'Дата доставки', header_format)
    worksheet.merge_range(2, 20, 3, 20, 'Дата следующего планового ремонта', header_format)
    worksheet.write_row(3, 4, ['Станция текущей дислокации',
                               'Дорога операции',
                               'Дата операции',
                               'Операция',
                               'Поезд',
                               'Индекс поезда',
                               'Простой',
                               'Станция отправления',
                               'Дорога отправления',
                               'Станция назначения',
                               'Дорога назначения'], header_format)

    # Данные
    line = 4
    partner = None
    feature = None
    for index, row in df.iterrows():
        if row['Partner_Name'] != partner:
            partner = row['Partner_Name']
            worksheet.merge_range(line, 0, line, 20, partner, partner_format)
            line += 1
        if row['Feature'] != feature:
            feature = row['Feature']
            worksheet.merge_range(line, 0, line, 20, feature, feature_format)
            worksheet.set_row(line, None, None, {'level': 1})
            line += 1
        worksheet.set_row(line, None, None, {'level': 2})
        worksheet.write(line, 0, row['Wagon'], wagon_format)
        worksheet.write(line, 1, row['Next_Load_Station_Name'], main_text_format)
        worksheet.write(line, 2, row['Capacity'], main_digit_2_format)
        worksheet.write(line, 3, row['Owner'], main_text_format)
        worksheet.write(line, 4, row['Operation_Station_Name'], main_text_format)
        worksheet.write(line, 5, row['Operation_Railway_Name'], main_text_format)
        worksheet.write(line, 6, str(row['Operation_Date']), main_text_format)
        worksheet.write(line, 7, row['Operation_Name'], main_text_format)
        worksheet.write(line, 8, row['Train_Number'], main_text_format)
        worksheet.write(line, 9, row['Train_Index'], main_text_format)
        worksheet.write(line, 10, row['Parking_Time'], main_digit_1_format)
        worksheet.write(line, 11, row['Departure_Station_Name'], main_text_format)
        worksheet.write(line, 12, row['Departure_Railway_Name'], main_text_format)
        worksheet.write(line, 13, row['Destination_Station_Name'], main_text_format)
        worksheet.write(line, 14, row['Destination_Railway_Name'], main_text_format)
        worksheet.write(line, 15, row['Remaining_Distance'], main_digit_0_format)
        worksheet.write(line, 16, row['Cargo_Name'], main_text_format)
        worksheet.write(line, 17, row['Cargo_Weight'], main_digit_0_format)
        worksheet.write(line, 18, str(row['Load_Date']) if str(row['Load_Date']) != 'NaT' else '', main_text_format)
        worksheet.write(line, 19, str(row['Delivery_Date']) if str(row['Delivery_Date']) != 'NaT' else '',
                        main_text_format)
        worksheet.write(line, 20, str(row['Next_SR_Date']), main_text_format)
        line += 1

    workbook.close()
    return filename


def get_database_connection():
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']
    conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + \
               f'{server};DATABASE={database};UID={username};PWD={password}'
    return pyodbc.connect(conn_str, attrs_before={}, timeout=0)


def get_reply_keyboard(keyboard_type=None):
    if keyboard_type == 'report':
        button = KeyboardButton(GET_REPORT_TEXT)
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        keyboard.add(button)
    elif keyboard_type == 'contact':
        button = KeyboardButton('Сообщить боту номер телефона', request_contact=True)
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        keyboard.add(button)
    elif keyboard_type == 'yes_no':
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        button_yes = KeyboardButton('Да')
        button_no = KeyboardButton('Нет')
        keyboard.add(button_yes, button_no)
    else:
        keyboard = ReplyKeyboardRemove()
    return keyboard


def get_inline_keyboard(df, mode='partner'):
    keyboard = InlineKeyboardMarkup(row_width=1)
    if mode == 'partner':
        for row in df.iterrows():
            partner = row[1]['full_name']
            ref = row[1]['ref'].hex()
            button = InlineKeyboardButton(partner, callback_data=json.dumps({"type": "partner", "ref": ref}))
            keyboard.add(button)
        keyboard.add(InlineKeyboardButton('Все', callback_data=json.dumps({"type": "partner", "ref": "all"})))
    elif mode in ('cargo', 'package', 'wagon'):
        for _, row in df.iterrows():
            code = row['Code']
            name = row['Name']
            # if mode == 'cargo':
            #     cargos[code] = name
            # elif mode == 'package':
            #     packages[code] = name
            # else:
            #     wagons[code] = name
            button = InlineKeyboardButton(f'{code} - {name}',
                                          callback_data=json.dumps({"type": mode, "code": code}))
            keyboard.add(button)
    elif mode == 'station':
        for _, row in df.iterrows():
            code = row['Code']
            name = row['Name']
            road_name = row['RW_Name']
            # stations[code] = {'name': name, 'road_name': road_name}
            button = InlineKeyboardButton(f'{code} - {name} ({road_name})',
                                          callback_data=json.dumps({"type": 'station', "code": code}))
            keyboard.add(button)
    return keyboard


def store_data(store, data):
    for _, row in data.iterrows():
        store[row['Code']] = row['Name']


def store_stations_data(store, data):
    for _, row in data.iterrows():
        store[row['Code']] = {'name': row['Name'], 'road_name': row['RW_Name']}


def get_calculate_price_request_xml(request_data):
    request_xml = CALCULATE_PRICE_XML.strip()
    for key in request_data.keys():
        request_xml = request_xml.replace('%' + key + '%', str(request_data[key]))
    # request_xml = request_xml.replace('%cargo_type_code%', request_data['cargo_type_code'])
    # request_xml = request_xml.replace('%cargo_weight%', request_data['cargo_weight'])
    # request_xml = request_xml.replace('%cargo_volume%', request_data['cargo_volume'])
    # request_xml = request_xml.replace('%cargo_package_type_code%', request_data['cargo_package_type_code'])
    # request_xml = request_xml.replace('%start_date%', request_data['start_date'])
    # request_xml = request_xml.replace('%finish_date%', request_data['finish_date'])
    # request_xml = request_xml.replace('%departure_station_code%', request_data['departure_station_code'])
    # request_xml = request_xml.replace('%destination_station_code%', request_data['destination_station_code'])
    # request_xml = request_xml.replace('%wagon_type_code%', request_data['wagon_type_code'])
    return request_xml


bot.polling(none_stop=True)
