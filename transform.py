from utils import *
from sql import *

from numpy import unique


def transform_notifications(xml, region_id):
    document = dict()

    document['finSource'] = retrieve(xml, './s:lot/s:financeSource/text()', str)
    if document['finSource']:
        document['finSource'].strip().strip('.').lower()
    document['finSource'] = truncate_text(document['finSource'], 1000)
    try:
        fin_id, = one_row_request("INSERT INTO finance_sources (source) VALUES (%s) RETURNING id;",
                                  [document['finSource']], if_commit=True)
    except psycopg2.IntegrityError:
        fin_id, = one_row_request("SELECT id from finance_sources WHERE source = %s;", [document['finSource']])

    document['procurerRegNum'] = retrieve(xml, './s:purchaseResponsible/s:responsibleOrg/s:regNum/text()', str)
    document['procurerName'] = retrieve(xml, './s:purchaseResponsible/s:responsibleOrg/s:fullName/text()', str)
    document['procurerName'] = truncate_text(document['procurerName'], 1000)
    document['procurerINN'] = retrieve(xml, './s:purchaseResponsible/s:responsibleOrg/s:INN/text()', str)
    no_return_command("INSERT INTO procurers (reg_num, INN, name) VALUES (%s, %s, %s) ON CONFLICT (reg_num) "
                      "DO NOTHING;", [document['procurerRegNum'], document['procurerINN'], document['procurerName']])

    document['purchaseNumber'] = retrieve(xml, './s:purchaseNumber/text()', str)
    document['startDate'] = retrieve(xml, './s:procedureInfo/s:collecting/s:startDate/text()', parse_datetime)
    document['endDate'] = retrieve(xml, './s:procedureInfo/s:collecting/s:endDate/text()', parse_datetime)
    document['maxPrice'] = retrieve(xml, './s:lot/s:maxPrice/text()', lambda x: round(float(x), 2))
    document['currency'] = retrieve(xml, './s:lot/s:maxPrice/s:currency/s:code/text()', str)

    document['deliveryTerm'] = retrieve(xml, './s:lot/s:customerRequirements/s:customerRequirement')
    document['deliveryTerm'] = retrieve(document['deliveryTerm'], './s:deliveryTerm/text()', str)
    document['deliveryTerm'] = truncate_text(document['deliveryTerm'], 200)

    auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, start_date, end_date, max_price, "
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

    if retrieve(xml, './s:drugPurchaseObjectInfo'):
        objects = [['00', '00', '00', '000']]
        obj_names = ['Drugs']
        if_okpd2 = [False]
    else:
        objects = []
        obj_names = []
        if_okpd2 = []
    for obj_xml in xml.xpath('./s:lot/s:purchaseObjects/s:purchaseObject', namespaces=ns()):
        okpd2_code = retrieve(obj_xml, './s:OKPD2/s:code/text()', str)
        if okpd2_code is not None:
            okpd2_name = retrieve(obj_xml, './s:OKPD2/s:name/text()', str)
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
        try:
            obj_id, = one_row_request("INSERT INTO purchase_objects (code_1, code_2, code_3, code_4, if_OKPD2, "
                                      "name) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                                      [obj[0], obj[1], obj[2], obj[3], if_okpd2_cur, obj_name], if_commit=True)
        except psycopg2.IntegrityError:
            obj_id, = one_row_request("SELECT id FROM purchase_objects WHERE name = %s;", [obj_name])
        no_return_command("INSERT INTO auction_purchase_objects (auction_id, purchase_object_id) "
                          "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, obj_id])  # index out of range


def transform_notifications_prolong(xml, region_id):
    purchase_number = retrieve(xml, './s:purchaseNumber/text()', str)
    prolong_date = retrieve(xml, './s:collectingProlongationDate/text()', parse_datetime)
    no_return_command("INSERT INTO auctions (region_id, purchase_number, prolong_date) "
                      "VALUES (%s, %s, %s) ON CONFLICT (purchase_number) DO UPDATE SET "
                      "prolong_date = %s;",
                      [region_id, purchase_number, prolong_date, prolong_date])


def transform_protocols(xml, region_id, if_prolong=False):
    purchase_number = retrieve(xml, './s:purchaseNumber/text()', str)
    n_commission_members = len(xml.xpath('./s:commission/s:commissionMembers/s:commissionMember', namespaces=ns()))
    auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, n_commission_members) "
                                  "VALUES (%s, %s, %s) ON CONFLICT (purchase_number) DO UPDATE "
                                  "SET n_commission_members = %s RETURNING id;",
                                  [region_id, purchase_number, n_commission_members, n_commission_members],
                                  if_commit=True)

    for i, application_xml in enumerate(xml.xpath('./s:protocolLot/s:applications/s:application', namespaces=ns())):
        bid_price = retrieve(application_xml, './s:price/text()', lambda x: round(float(x), 2))
        bid_time = retrieve(application_xml, './s:appDate/text()', parse_datetime)
        part_inn = retrieve(application_xml, './s:appParticipant/s:inn/text()', str)
        part_name = retrieve(application_xml, './s:appParticipant/s:organizationName/text()', str)
        part_name = truncate_text(part_name, 1000)

        correspondences = application_xml.xpath('./s:correspondencies/s:correspondence', namespaces=ns())
        if i == 0:
            is_approved = parse_correspondences(auction_id, correspondences)

        else:
            is_approved = True
            for correspondence in correspondences:
                compatible = True if retrieve(correspondence, './s:compatible/text()', str) == 'true' else False
                if not compatible:
                    is_approved = False
                    break

        no_return_command("INSERT INTO participants (INN, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                          [part_inn, part_name])
        no_return_command("INSERT INTO bids (auction_id, participant_INN, price, date, is_approved, is_after_prolong) "
                          "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                          [auction_id, part_inn, bid_price, bid_time, is_approved, if_prolong])


def parse_correspondences(auction_id, correspondences):
    is_approved = True
    for correspondence in correspondences:
        if is_approved:
            compatible = True if retrieve(correspondence, './s:compatible/text()', str) == 'true' else False
            if not compatible:
                is_approved = False

        requirement = retrieve(correspondence, './s:requirement')
        if requirement is None:
            requirement = retrieve(correspondence, './s:preferense')
        if requirement is None:
            requirement = retrieve(correspondence, './s:restriction')
        req_code = retrieve(requirement, './s:code/text()', str)
        req_name = retrieve(requirement, './s:name/text()', str)
        req_name = truncate_text(req_name, 1000)
        req_id = None

        # correspondences info probably should have been taken from notifications, but it's not a big deal
        if req_code is None:
            req_short_name = retrieve(requirement, './s:shortName/text()', str)
            if req_short_name:
                req_id, = one_row_request("INSERT INTO correspondences (name, short_name) "
                                          "VALUES (%s, %s) ON CONFLICT (name) "
                                          "DO UPDATE SET short_name = %s RETURNING id;",
                                          [req_name, req_short_name, req_short_name], if_commit=True)
        else:
            req_id, = one_row_request("INSERT INTO correspondences (name, code) "
                                      "VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET code = %s RETURNING id;",
                                      [req_name, req_code, req_code], if_commit=True)

        if req_id:
            no_return_command("INSERT INTO auction_correspondences (auction_id, correspondence_id) "
                              "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, req_id])
        return is_approved


def transform_new_notifications_504(xml, region_id):
    document = dict()

    document['finSource'] = retrieve(xml,
                                     './ep:notificationInfo/ep:customerRequirementsInfo/ep:customerRequirementInfo/ep:contractConditionsInfo/ep:contractExecutionPaymentPlan/ep:financingSourcesInfo/text()',
                                     str)
    if document['finSource']:
        document['finSource'].strip().strip('.').lower()
    document['finSource'] = truncate_text(document['finSource'], 1000)
    try:
        fin_id, = one_row_request("INSERT INTO finance_sources (source) VALUES (%s) RETURNING id;",
                                  [document['finSource']], if_commit=True)
    except psycopg2.IntegrityError:
        fin_id, = one_row_request("SELECT id from finance_sources WHERE source = %s;", [document['finSource']])

    document['procurerRegNum'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:regNum/text()',
                                          str)
    document['procurerName'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:fullName/text()',
                                        str)
    document['procurerName'] = truncate_text(document['procurerName'], 1000)
    document['procurerINN'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:INN/text()', str)
    no_return_command("INSERT INTO procurers (reg_num, INN, name) VALUES (%s, %s, %s) ON CONFLICT (reg_num) "
                      "DO NOTHING;", [document['procurerRegNum'], document['procurerINN'], document['procurerName']])

    document['purchaseNumber'] = retrieve(xml, './ep:commonInfo/ep:purchaseNumber/text()', str)
    document['startDate'] = retrieve(xml, './ep:notificationInfo/ep:procedureInfo/ep:collectingInfo/ep:startDT/text()',
                                     parse_datetime)
    document['endDate'] = retrieve(xml, './ep:notificationInfo/ep:procedureInfo/ep:collectingInfo/ep:endDT/text()',
                                   parse_datetime)
    document['maxPrice'] = retrieve(xml,
                                    './ep:notificationInfo/ep:contractConditionsInfo/ep:maxPriceInfo/ep:maxPrice/text()',
                                    lambda x: round(float(x), 2))
    document['currency'] = retrieve(xml,
                                    './ep:notificationInfo/ep:contractConditionsInfo/ep:maxPriceInfo/ep:currency/base:code/text()',
                                    str)

    document['deliveryTerm'] = retrieve(xml,
                                        './ep:notificationInfo/ep:customerRequirementsInfo/ep:customerRequirementInfo/ep:contractConditionsInfo/ep:deliveryPlacesInfo/ep:deliveryPlaceInfo/ep:deliveryPlace/text()',
                                        str)
    document['deliveryTerm'] = truncate_text(document['deliveryTerm'], 200)

    auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, start_date, end_date, max_price, "
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

    if retrieve(xml, './ep:purchaseObjectsInfo/ep:drugPurchaseObjectsInfo/ep:drugPurchaseObjectInfo'):
        objects = [['00', '00', '00', '000']]
        obj_names = ['Drugs']
        if_okpd2 = [False]
    else:
        objects = []
        obj_names = []
        if_okpd2 = []
    for obj_xml in xml.xpath(
            './ep:notificationInfo/ep:purchaseObjectsInfo/ep:notDrugPurchaseObjectsInfo/com:purchaseObject',
            namespaces=ns()):
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
        try:
            obj_id, = one_row_request("INSERT INTO purchase_objects (code_1, code_2, code_3, code_4, if_OKPD2, "
                                      "name) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                                      [obj[0], obj[1], obj[2], obj[3], if_okpd2_cur, obj_name], if_commit=True)
        except psycopg2.IntegrityError:
            obj_id, = one_row_request("SELECT id FROM purchase_objects WHERE name = %s;", [obj_name])
        no_return_command("INSERT INTO auction_purchase_objects (auction_id, purchase_object_id) "
                          "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, obj_id])  # index out of range


def transform_new_notifications_ep(xml, region_id):
    document = dict()

    document['finSource'] = retrieve(xml,
                                     './ep:notificationInfo/ep:customerRequirementsInfo/ep:customerRequirementInfo/ep:contractConditionsInfo/ep:contractExecutionPaymentPlan/ep:financingSourcesInfo/text()',
                                     str)
    if document['finSource']:
        document['finSource'].strip().strip('.').lower()
    document['finSource'] = truncate_text(document['finSource'], 1000)
    try:
        fin_id, = one_row_request("INSERT INTO finance_sources (source) VALUES (%s) RETURNING id;",
                                  [document['finSource']], if_commit=True)
    except psycopg2.IntegrityError:
        fin_id, = one_row_request("SELECT id from finance_sources WHERE source = %s;", [document['finSource']])

    document['procurerRegNum'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:regNum/text()',
                                          str)
    document['procurerName'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:fullName/text()',
                                        str)
    document['procurerName'] = truncate_text(document['procurerName'], 1000)
    document['procurerINN'] = retrieve(xml, './ep:purchaseResponsibleInfo/ep:responsibleOrgInfo/ep:INN/text()', str)
    no_return_command("INSERT INTO procurers (reg_num, INN, name) VALUES (%s, %s, %s) ON CONFLICT (reg_num) "
                      "DO NOTHING;", [document['procurerRegNum'], document['procurerINN'], document['procurerName']])

    document['purchaseNumber'] = retrieve(xml, './ep:commonInfo/ep:purchaseNumber/text()', str)
    document['startDate'] = retrieve(xml, './ep:notificationInfo/ep:procedureInfo/ep:collectingInfo/ep:startDT/text()',
                                     parse_datetime)
    document['endDate'] = retrieve(xml, './ep:notificationInfo/ep:procedureInfo/ep:collectingInfo/ep:endDT/text()',
                                   parse_datetime)
    document['maxPrice'] = retrieve(xml,
                                    './ep:notificationInfo/ep:contractConditionsInfo/ep:maxPriceInfo/ep:maxPrice/text()',
                                    lambda x: round(float(x), 2))
    document['currency'] = retrieve(xml,
                                    './ep:notificationInfo/ep:contractConditionsInfo/ep:maxPriceInfo/ep:currency/base:'
                                    'code/text()',
                                    str)

    document['deliveryTerm'] = retrieve(xml,
                                        './ep:notificationInfo/ep:customerRequirementsInfo/ep:customerRequirementInfo/ep'
                                        ':contractConditionsInfo/ep:deliveryPlacesInfo/ep:deliveryPlaceInfo/ep'
                                        ':deliveryPlace/text()',
                                        str)
    document['deliveryTerm'] = truncate_text(document['deliveryTerm'], 200)

    auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, start_date, end_date, max_price, "
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

    if retrieve(xml, './ep:drugPurchaseObjectInfo'):
        objects = [['00', '00', '00', '000']]
        obj_names = ['Drugs']
        if_okpd2 = [False]
    else:
        objects = []
        obj_names = []
        if_okpd2 = []
    for obj_xml in xml.xpath(
            './ep:notificationInfo/ep:purchaseObjectsInfo/ep:notDrugPurchaseObjectsInfo/com:purchaseObject',
            namespaces=ns()):
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
        try:
            obj_id, = one_row_request("INSERT INTO purchase_objects (code_1, code_2, code_3, code_4, if_OKPD2, "
                                      "name) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                                      [obj[0], obj[1], obj[2], obj[3], if_okpd2_cur, obj_name], if_commit=True)
        except psycopg2.IntegrityError:
            obj_id, = one_row_request("SELECT id FROM purchase_objects WHERE name = %s;", [obj_name])
        no_return_command("INSERT INTO auction_purchase_objects (auction_id, purchase_object_id) "
                          "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, obj_id])  # index out of range


def transform_new_protocols_ezk2(xml, region_id, if_prolong=False):
    purchase_number = retrieve(xml, './ep:commonInfo/ep:purchaseNumber/text()', str)
    n_commission_members = len(
        xml.xpath('./ep:protocolInfo/ep:commissionInfo/com:commissionMembers/com:commissionMember', namespaces=ns()))
    auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, n_commission_members) "
                                  "VALUES (%s, %s, %s) ON CONFLICT (purchase_number) DO UPDATE "
                                  "SET n_commission_members = %s RETURNING id;",
                                  [region_id, purchase_number, n_commission_members, n_commission_members],
                                  if_commit=True)

    for i, application_xml in enumerate(
            xml.xpath('./ep:protocolInfo/ep:applicationsInfo/ep:applicationInfo', namespaces=ns())):
        inn = retrieve(application_xml, './ep:appParticipantInfo/com:legalEntityRFInfo/com:INN/text()', str)
        part_name = retrieve(application_xml, './ep:appParticipantInfo/com:legalEntityRFInfo/com:fullName/text()', str)
        if inn == None:
            inn = retrieve(application_xml, './ep:appParticipantInfo/com:individualPersonRFInfo/com:INN/text()', str)
            part_name = retrieve(application_xml,
                                 './ep:appParticipantInfo/com:individualPersonRFInfo/com:nameInfo/com:lastName/text()',
                                 str)
        part_name = truncate_text(part_name, 1000)
        bid_price = retrieve(application_xml, './ep:finalPrice/text()', lambda x: round(float(x), 2))
        bid_time = retrieve(application_xml, './ep:commonInfo/ep:appDT/text()', parse_datetime)
        correspondences = retrieve(application_xml, './ep:admittedInfo/ep:appAdmittedInfo/ep:admitted/text()', str)
        no_return_command("INSERT INTO participants (INN, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                          [inn, part_name])
        no_return_command("INSERT INTO bids (auction_id, participant_INN, price, date, is_approved, is_after_prolong) "
                          "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                          [auction_id, inn, bid_price, bid_time, correspondences, if_prolong])


def transform_new_protocols_finalpart(xml, region_id, if_prolong=False):
    purchase_number = retrieve(xml, './ep:commonInfo/ep:purchaseNumber/text()', str)
    n_commission_members = len(
        xml.xpath('./ep:protocolInfo/ep:commissionInfo/com:commissionMembers/com:commissionMember', namespaces=ns()))

    auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, n_commission_members) "
                                  "VALUES (%s, %s, %s) ON CONFLICT (purchase_number) DO UPDATE "
                                  "SET n_commission_members = %s RETURNING id;",
                                  [region_id, purchase_number, n_commission_members, n_commission_members],
                                  if_commit=True)

    inn = []
    name_part = []
    for i, application2_xml in enumerate(xml.xpath('./ep:appParticipantsInfo/ep:appParticipantInfo', namespaces=ns())):
        inn.append(retrieve(application2_xml, './ep:participantInfo/com:legalEntityRFInfo/com:INN/text()', str))
        name_part.append(
            retrieve(application2_xml, './ep:participantInfo/com:legalEntityRFInfo/com:fullName/text()', str))
        if inn[i] == None:
            inn[i] = retrieve(application2_xml, './ep:participantInfo/com:individualPersonRFInfo/com:INN/text()',
                              str)
            name_part[i] = retrieve(application2_xml,
                                    './ep:participantInfo/com:individualPersonRFInfo/com:nameInfo/com:lastName/text()',
                                    str)
        name_part[i] = truncate_text(name_part[i], 1000)

    for i, application_xml in enumerate(
            xml.xpath('./ep:protocolInfo/ep:applicationsInfo/ep:applicationInfo', namespaces=ns())):
        bid_price = retrieve(application_xml, './ep:finalPrice/text()', lambda x: round(float(x), 2))
        bid_time = retrieve(application_xml, './ep:commonInfo/ep:appDT/text()', parse_datetime)
        correspondences = application_xml.xpath('./ep:correspondenciesInfo/ep:correspondenceInfo', namespaces=ns())
        if i == 0:
            is_approved = parse_new_correspondences(auction_id, correspondences)
        else:
            is_approved = True
            for correspondence in correspondences:
                compatible = True if retrieve(correspondence, './ep:compatible/text()', str) == 'true' else False
                if not compatible:
                    is_approved = False
                    break
        no_return_command("INSERT INTO participants (INN, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                          [inn[i], name_part[i]])
        no_return_command("INSERT INTO bids (auction_id, participant_INN, price, date, is_approved, is_after_prolong) "
                          "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                          [auction_id, inn[i], bid_price, bid_time, is_approved, if_prolong])


def parse_new_correspondences(auction_id, correspondences):
    is_approved = True
    for correspondence in correspondences:
        if is_approved:
            compatible = True if retrieve(correspondence, './ep:compatible/text()', str) == 'true' else False
            if not compatible:
                is_approved = False

        requirement = retrieve(correspondence, './ep:preferenseInfo/com:preferenseRequirementInfo')
        # if requirement is None:
        # requirement = retrieve(correspondence, './s:preferense')
        # if requirement is None:
        # requirement = retrieve(correspondence, './s:restriction') позже чекнуть
        req_code = retrieve(requirement, './base:code/text()', str)
        req_name = retrieve(requirement, './base:name/text()', str)
        req_name = truncate_text(req_name, 1000)
        req_id = None

        # correspondences info probably should have been taken from notifications, but it's not a big deal
        if req_code is None:
            req_short_name = retrieve(requirement, './base:shortName/text()', str)
            if req_short_name:
                req_id, = one_row_request("INSERT INTO correspondences (name, short_name) "
                                          "VALUES (%s, %s) ON CONFLICT (name) "
                                          "DO UPDATE SET short_name = %s RETURNING id;",
                                          [req_name, req_short_name, req_short_name], if_commit=True)
        else:
            req_id, = one_row_request("INSERT INTO correspondences (name, code) "
                                      "VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET code = %s RETURNING id;",
                                      [req_name, req_code, req_code], if_commit=True)

        if req_id:
            no_return_command("INSERT INTO auction_correspondences (auction_id, correspondence_id) "
                              "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, req_id])
        return is_approved
