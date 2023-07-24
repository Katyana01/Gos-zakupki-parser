import xml.etree.ElementTree as ET
# import lxml.etree
from lxml import etree

from utils import *
from sql import *

from numpy import unique




tree = ET.parse("C:/Users/artem/OneDrive/Рабочий стол/epProtocolEZK2020FinalPart_0378300000121000174_36132279.xml")
root = tree.getroot()
regnum = tree.find(
    '{http://zakupki.gov.ru/oos/export/1}epProtocolEZK2020FinalPart/{http://zakupki.gov.ru/oos/EPtypes/1}appParticipantsInfo')
for child in regnum:
    print(child.tag, child.attrib)

def transform_new_notifications(xml, region_id):
    document = dict()

    document['finSource'] = retrieve(xml, './ep:notificationInfo/ep:customerRequirementsInfo/ep:customerRequirementInfo/ep:contractConditionsInfo/ep:contractExecutionPaymentPlan/ep:financingSourcesInfo/text()', str)
    if document['finSource']:
        document['finSource'].strip().strip('.').lower()
    document['finSource'] = truncate_text(document['finSource'], 1000)
    '''try:
        fin_id, = one_row_request("INSERT INTO finance_sources (source) VALUES (%s) RETURNING id;",
                                 # [document['finSource']], if_commit=True)
    except psycopg2.IntegrityError:
        fin_id, = one_row_request("SELECT id from finance_sources WHERE source = %s;", [document['finSource']])
    '''
    document['procurerRegNum'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:regNum/text()', str)
    document['procurerName'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:fullName/text()', str)
    document['procurerName'] = truncate_text(document['procurerName'], 1000)
    document['procurerINN'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:INN/text()', str)
    #no_return_command("INSERT INTO procurers (reg_num, INN, name) VALUES (%s, %s, %s) ON CONFLICT (reg_num) "
                     # "DO NOTHING;", [document['procurerRegNum'], document['procurerINN'], document['procurerName']])

    document['purchaseNumber'] = retrieve(xml, './ep:commonInfo/ep:purchaseNumber/text()', str)
    document['startDate'] = retrieve(xml, './ep:notificationInfo/ep:procedureInfo/ep:collectingInfo/ep:startDT/text()', parse_datetime)
    document['endDate'] = retrieve(xml, './ep:notificationInfo/ep:procedureInfo/ep:collectingInfo/ep:endDT/text()', parse_datetime)
    document['maxPrice'] = retrieve(xml, './ep:notificationInfo/ep:contractConditionsInfo/ep:maxPriceInfo/ep:maxPrice/text()', lambda x: round(float(x), 2))
    document['currency'] = retrieve(xml, './ep:notificationInfo/ep:contractConditionsInfo/ep:maxPriceInfo/ep:currency/base:code/text()', str)

    document['deliveryTerm'] = retrieve(xml, './ep:notificationInfo/ep:customerRequirementsInfo/ep:customerRequirementInfo/ep:contractConditionsInfo/ep:deliveryPlacesInfo/ep:deliveryPlaceInfo/ep:deliveryPlace/text()', str)
    document['deliveryTerm'] = truncate_text(document['deliveryTerm'], 200)
    print(document)

    '''auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, start_date, end_date, max_price, "
                                  "currency, procurer_reg_num, finance_source_id, delivery_term) "
                                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (purchase_number) "
                                  "DO UPDATE SET start_date = %s, end_date = %s, max_price = %s, "
                                  "currency = %s, procurer_reg_num = %s, finance_source_id = %s, "
                                  "delivery_term = %s RETURNING id;",
                                  [region_id, document['purchaseNumber'], document['startDate'], document['endDate'],
                                   document['maxPrice'], document['currency'], document['procurerRegNum'], fin_id,
                                   document['deliveryTerm'], document['startDate'], document['endDate'],
                                   document['maxPrice'], document['currency'], document['procurerRegNum'], fin_id,
                                   document['deliveryTerm']], if_commit=True)  # index out of range

    '''
    if retrieve(xml, './ep:purchaseObjectsInfo/ep:drugPurchaseObjectsInfo/ep:drugPurchaseObjectInfo'):
        objects = [['00', '00', '00', '000']]
        obj_names = ['Drugs']
        if_okpd2 = [False]
    else:
        objects = []
        obj_names = []
        if_okpd2 = []
    for obj_xml in xml.xpath('./ep:notificationInfo/ep:purchaseObjectsInfo/ep:notDrugPurchaseObjectsInfo/com:purchaseObject', namespaces=ns()):
        okpd2_code = retrieve(obj_xml, './com:OKPD2/base:OKPDCode/text()', str)
        okpd2_name = retrieve(obj_xml, './com:OKPD2/base:OKPDName/text()', str)
        if_okpd2.append(True)
        if okpd2_code is not None:
            objects.append(okpd2_code)
            okpd2_name = truncate_text(okpd2_name, 1000)
            obj_names.append(okpd2_name)
        else:
            if_okpd2.pop()

    if len(objects) > 0:
        objects, indices = unique(objects, return_index=True)
    for i, obj in enumerate(objects):
        obj = obj.split('.')
        while len(obj) < 4:
            obj.append('00')
        obj_name = obj_names[indices[i]]
        if_okpd2_cur = if_okpd2[indices[i]]
    print(okpd2_code)
    '''try:
            obj_id, = one_row_request("INSERT INTO purchase_objects (code_1, code_2, code_3, code_4, if_OKPD2, "
                                      "name) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                                       [obj[0], obj[1], obj[2], obj[3], if_okpd2_cur, obj_name], if_commit=True)
        except psycopg2.IntegrityError:
            obj_id, = one_row_request("SELECT id FROM purchase_objects WHERE name = %s;", [obj_name])
        no_return_command("INSERT INTO auction_purchase_objects (auction_id, purchase_object_id) "
                          "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, obj_id])  # index out of range
    '''

xml_file = etree.iterparse(
    "C:/Users/artem/OneDrive/Рабочий стол/epNotificationEZK2020_0320200029323000002_31554356.xml",
    tag='{http://zakupki.gov.ru/oos/export/1}epNotificationEZK2020')

def transform_new_notifications_ep(xml, region_id):
    document = dict()

    document['finSource'] = retrieve(xml, './ep:notificationInfo/ep:customerRequirementsInfo/ep:customerRequirementInfo/ep:contractConditionsInfo/ep:contractExecutionPaymentPlan/ep:financingSourcesInfo/text()', str)
    '''if document['finSource']:
        document['finSource'].strip().strip('.').lower()
    document['finSource'] = truncate_text(document['finSource'], 1000)
    try:
        fin_id, = one_row_request("INSERT INTO finance_sources (source) VALUES (%s) RETURNING id;",
                                  [document['finSource']], if_commit=True)
    except psycopg2.IntegrityError:
        fin_id, = one_row_request("SELECT id from finance_sources WHERE source = %s;", [document['finSource']])
    '''
    document['procurerRegNum'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:regNum/text()', str)
    document['procurerName'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:fullName/text()', str)
    document['procurerName'] = truncate_text(document['procurerName'], 1000)
    document['procurerINN'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:INN/text()', str)
    '''no_return_command("INSERT INTO procurers (reg_num, INN, name) VALUES (%s, %s, %s) ON CONFLICT (reg_num) "
                      "DO NOTHING;", [document['procurerRegNum'], document['procurerINN'], document['procurerName']])
    '''
    document['purchaseNumber'] = retrieve(xml, './ep:commonInfo/ep:purchaseNumber/text()', str)
    document['startDate'] = retrieve(xml, './ep:notificationInfo/ep:procedureInfo/ep:collectingInfo/ep:startDT/text()', parse_datetime)
    document['endDate'] = retrieve(xml, './ep:notificationInfo/ep:procedureInfo/ep:collectingInfo/ep:endDT/text()', parse_datetime)
    document['maxPrice'] = retrieve(xml, './ep:notificationInfo/ep:contractConditionsInfo/ep:maxPriceInfo/ep:maxPrice/text()', lambda x: round(float(x), 2))
    document['currency'] = retrieve(xml, './ep:notificationInfo/ep:contractConditionsInfo/ep:maxPriceInfo/ep:currency/base:code/text()', str)

    document['deliveryTerm'] = retrieve(xml, './ep:notificationInfo/ep:customerRequirementsInfo/ep:customerRequirementInfo/ep:contractConditionsInfo/ep:deliveryPlacesInfo/ep:deliveryPlaceInfo/ep:deliveryPlace/text()', str)
    document['deliveryTerm'] = truncate_text(document['deliveryTerm'], 200)
    print(document)
    '''auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, start_date, end_date, max_price, "
                                  "currency, procurer_reg_num, finance_source_id, delivery_term) "
                                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (purchase_number) "
                                  "DO UPDATE SET start_date = %s, end_date = %s, max_price = %s, "
                                  "currency = %s, procurer_reg_num = %s, finance_source_id = %s, "
                                  "delivery_term = %s RETURNING id;",
                                  [region_id, document['purchaseNumber'], document['startDate'], document['endDate'],
                                   document['maxPrice'], document['currency'], document['procurerRegNum'], fin_id,
                                   document['deliveryTerm'], document['startDate'], document['endDate'],
                                   document['maxPrice'], document['currency'], document['procurerRegNum'], fin_id,
                                   document['deliveryTerm']], if_commit=True)  # index out of range
    '''

    if retrieve(xml, './ep:drugPurchaseObjectInfo'):
        objects = [['00', '00', '00', '000']]
        obj_names = ['Drugs']
        if_okpd2 = [False]
    else:
        objects = []
        obj_names = []
        if_okpd2 = []
    for obj_xml in xml.xpath('./ep:notificationInfo/ep:purchaseObjectsInfo/ep:notDrugPurchaseObjectsInfo/com:purchaseObject', namespaces=ns()):
        okpd2_code = retrieve(obj_xml, './com:KTRU/com:OKPD2/base:OKPDCode/text()', str)
        if okpd2_code is not None:
            okpd2_name = retrieve(obj_xml, './com:KTRU/com:OKPD2/base:OKPDName/text()', str)
            if_okpd2.append(True)
        else:
            okpd2_code = retrieve(obj_xml, './s:OKPD/s:code/text()', str)
            okpd2_name = retrieve(obj_xml, './s:OKPD/s:name/text()', str)
            if_okpd2.append(False)
        if okpd2_code is not None:
            objects.append(okpd2_code)
            okpd2_name = truncate_text(okpd2_name, 1000)
            obj_names.append(okpd2_name)
        else:
            if_okpd2.pop()

    if len(objects) > 0:
        objects, indices = unique(objects, return_index=True)
    for i, obj in enumerate(objects):
        obj = obj.split('.')
        while len(obj) < 4:
            obj.append('00')
        obj_name = obj_names[indices[i]]
        if_okpd2_cur = if_okpd2[indices[i]]
        '''try:
            obj_id, = one_row_request("INSERT INTO purchase_objects (code_1, code_2, code_3, code_4, if_OKPD2, "
                                      "name) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                                      [obj[0], obj[1], obj[2], obj[3], if_okpd2_cur, obj_name], if_commit=True)
        except psycopg2.IntegrityError:
            obj_id, = one_row_request("SELECT id FROM purchase_objects WHERE name = %s;", [obj_name])
        no_return_command("INSERT INTO auction_purchase_objects (auction_id, purchase_object_id) "
                          "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, obj_id])  # index out of range
        '''



for event, xml in xml_file:
    if event == 'end':
        transform_new_notifications_ep(xml, 1)

'''
tree = ET.parse("C:/Users/artem/OneDrive/Рабочий стол/epProtocolEZK2020FinalPart_0378300000121000174_36132279.xml")
root = tree.getroot()
regnum = tree.find(
    '{http://zakupki.gov.ru/oos/export/1}epProtocolEZK2020FinalPart/{http://zakupki.gov.ru/oos/EPtypes/1}protocolInfo/{http://zakupki.gov.ru/oos/EPtypes/1}applicationsInfo/{http://zakupki.gov.ru/oos/EPtypes/1}applicationInfo')


for child in regnum:
    print(child.tag, child.attrib)


# def transform_notifications_ep(xml):
# print(xml.xpath('./{http://zakupki.gov.ru/oos/EPtypes/1}purchaseResponsibleInfo/{http://zakupki.gov.ru/oos/EPtypes/1}responsibleOrgInfo/{http://zakupki.gov.ru/oos/EPtypes/1}regNum/text()', smart_strings=False))
# return

# parser = lxml.etree.XMLParser(recover=True)
# tree = lxml.etree.fromstring("C:/Users/artem/OneDrive/Рабочий стол/fksNotificationOK504_0578300000121000001_25520167.xml", parser)
# [element.text for element in tree.iter('{http://zakupki.gov.ru/oos/EPtypes/1}regNum')]
# parsed = etree.iterparse("C:/Users/artem/OneDrive/Рабочий стол/fksNotificationOK504_0578300000121000001_25520167.xml", tag = '{http://zakupki.gov.ru/oos/export/1}epNotificationEOK')
# for event, xml in parsed:
# if event == 'end':
# transform_notifications_ep(xml)

def ns():  # XML namespace
    return {
        'exp': 'http://zakupki.gov.ru/oos/export/1',
        's': 'http://zakupki.gov.ru/oos/types/1',
        'int': 'http://zakupki.gov.ru/oos/integration/1',
        'print': 'http://zakupki.gov.ru/oos/printform/1',
        'ep': 'http://zakupki.gov.ru/oos/EPtypes/1',
        'com': 'http://zakupki.gov.ru/oos/common/1',
        'base': 'http://zakupki.gov.ru/oos/base/1'
    }


def retrieve(xml, xpath, fun=lambda x: x):
    try:
        ans = xml.xpath(xpath, namespaces=ns(), smart_strings=False)
        return fun(ans[0])
    except:
        # traceback.print_exc()
        return None


def transform_notifications_fcs(xml):
    i = 0
    for obj_xml in xml.xpath(
            './type:notificationInfo/type:purchaseObjectsInfo/type:notDrugPurchaseObjectsInfo/com:purchaseObject',
            namespaces=ns()):
        i = i + 1
        print(i)
    return

# parsed = etree.iterparse("C:/Users/artem/OneDrive/Рабочий стол/fcsNotificationOK504_0278100000220000097_24530555.xml",
# tag='{http://zakupki.gov.ru/oos/export/1}epNotificationEOK')
# for event, xml in parsed:
# if event == 'end':
# transform_notifications_fcs(xml)
'''
