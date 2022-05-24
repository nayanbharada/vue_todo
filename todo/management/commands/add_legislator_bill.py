import time
from datetime import datetime
import requests
from django.core.management import BaseCommand, call_command
from common.models import State
from directories.congress.models import (
    StateLegislatorBill,
    StateLegislatorBillAction,
    StateLegislator,
)
from django.db.models import Value as V
from django.db.models.functions import Concat
from directories.states.models import StateBillAPIKeyTracker, StateBillTracker, StateLegislativeSession
from utils.state_bill_text_mapper import state_bill_text_command_mapper


class Command(BaseCommand):
    """
    state legislator bill



    state_api = https://v3.openstates.org/jurisdictions?classification=state&page=1&per_page=52&apikey=d8f61d86-2cf3-4e53-a8f5-1fe65c0e752f"
    this api return jurisdiction_id of all states


    bill_lower_url = f"https://v3.openstates.org/bills?jurisdiction={jurisdiction_id}&sort=updated_desc&include=sponsorships&include=abstracts&include=other_titles&include=other_identifiers&include=actions&include=sources&include=documents&include=versions&include=votes&page=1&per_page=20&apikey={API_KEY}"
    response in all bill data return

    python manage.py add_legislator_bill --jurisdiction_id=ocd-jurisdiction/country:us/state:al/government
    juridiction_id pass

    """

    def add_arguments(self, parser):
        parser.add_argument("--api_key", type=str)
        parser.add_argument("--jurisdiction_id", type=str)
        parser.add_argument("--bill_page_no", type=int)
        parser.add_argument("--bill_id", type=str)

        """data will be available today date to that date Date formate: yyyy-mm-dd"""
        parser.add_argument("--created_since", type=str)

    def get_state(self, bill_content):
        state_data = bill_content.get("jurisdiction", "")
        state_obj = None
        if state_data:
            state_name = state_data.get("name", "")
            state_obj = State.objects.filter(name=state_name).first()
        return state_obj

    def get_date_from_datetime(self, date_string):
        if "T" in date_string:
            date = datetime.strptime(
                date_string.replace("+00:00", ""), "%Y-%m-%dT%H:%M:%S"
            ).date()
        else:
            date = date_string
        return date

    def get_chamber(self, bill_content):
        form_organization_data = bill_content.get("from_organization", "")
        chamber = ""
        if form_organization_data:
            chamber_name = form_organization_data.get("name", "")
            if chamber_name == "House":
                chamber = StateLegislatorBill.HOUSE
            else:
                chamber = StateLegislatorBill.SENATE
        return chamber

    def state_legislator(self, name, state_obj):
        state_legislator = (
            StateLegislator.objects.annotate(
                family_name=Concat("first_name", V(" "), "last_name")
            )
                .filter(family_name__icontains=name, legislator_state=state_obj)
                .first()
        )
        if state_legislator:
            return state_legislator
        else:
            return StateLegislator.objects.filter(
                full_response__name=name, legislator_state=state_obj
            ).first()

    def get_bill_sponsorship(self, sponsor, state_obj):
        """
        bill sponsor and co sponsor get
        :param sponsor:
        :param state_obj:
        :return:
        """
        sponsor_person = sponsor.get("person", "")
        state_legislator = None
        if sponsor_person:
            name = sponsor_person.get("name", "")
            if name:
                state_legislator = self.state_legislator(name, state_obj)
        else:
            name = sponsor.get("name", "")
            if name:
                state_legislator = self.state_legislator(name, state_obj)
        return state_legislator

    def get_state_legislators(self, sponsor_content, state_obj):
        """
        state legislator get
        :param sponsor_content:
        :param state_obj:
        :return:
        """
        sponsor_legislator_list = []
        co_sponsor_legislator_list = []
        for bill_sponsorship in sponsor_content:
            person_classification = bill_sponsorship.get("classification", "")
            if person_classification == "primary":
                legislator_obj = self.get_bill_sponsorship(bill_sponsorship, state_obj)
                if legislator_obj:
                    sponsor_legislator_list.append(legislator_obj.id)
            else:
                legislator_obj = self.get_bill_sponsorship(bill_sponsorship, state_obj)
                if legislator_obj:
                    co_sponsor_legislator_list.append(legislator_obj.id)
        return sponsor_legislator_list, co_sponsor_legislator_list

    def bill_actions_data(self, bill_data, state_legislator_bill_obj):
        """
        bill actions store
        :param bill_data:
        :param state_legislator_bill_obj:
        :return:
        """
        all_bill_actions = bill_data.get("actions", [])
        for bill_actions in all_bill_actions:
            bill_action_data = {}
            bill_action_description = bill_actions.get("description", "")
            bill_action_date = bill_actions.get("date", "")
            bill_action_order = bill_actions.get("order", "")
            bill_org_name = (
                bill_actions["organization"].get("name", "")
                if bill_actions.get("organization")
                else ""
            )
            bill_org_classification = (
                bill_actions["organization"].get("classification", "")
                if bill_actions.get("organization")
                else ""
            )
            bill_action_data.update(
                {
                    "state_legislator_bill": state_legislator_bill_obj,
                    "org_classification": bill_org_classification,
                    "org_name": bill_org_name,
                    "description": bill_action_description,
                    "order": bill_action_order,
                }
            )
            if bill_action_date:
                bill_action_date = self.get_date_from_datetime(bill_action_date)
                bill_action_data.update({"date": bill_action_date})

            bill_action_bill = StateLegislatorBillAction.objects.update_or_create(
                state_legislator_bill=state_legislator_bill_obj,
                order=bill_action_order,
                defaults=bill_action_data,
            )[0]
            if bill_action_bill:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"State legislature bill action add Bill id - {state_legislator_bill_obj.id} action id -- {bill_action_bill.id}"
                    )
                )
        return None

    def bill_text_data_store(self, state_legislator_bill_obj):
        state_bill_text_command_name = state_bill_text_command_mapper.get(state_legislator_bill_obj.state.name)
        if state_bill_text_command_name:
            call_command(state_bill_text_command_name, state_bill_obj_id=state_legislator_bill_obj.id)
        else:
            self.stdout.write(self.style.ERROR(
                f"state bill text mapper in state name not found. state_name:- {state_legislator_bill_obj.state.name}"))

    def get_legislative_session_obj(self, state_obj, session):
        """
        first statelegislativesession return
        :param state_obj:
        :param session:
        :return:
        """
        return StateLegislativeSession.objects.filter(state=state_obj, identifier=session).first()

    def store_state_legislature_bill(self, state_legislature_bill):
        """
        bill data get update in database
        :param state_legislature_bill:
        :return:
        """

        state_legislator_bill = {}
        bill_id = state_legislature_bill.get("id", "")
        session = state_legislature_bill.get("session", "")
        state_obj = self.get_state(state_legislature_bill)
        chamber = self.get_chamber(state_legislature_bill)
        identifier = state_legislature_bill.get("identifier", "")
        bill_title = state_legislature_bill.get("title", "")
        legislators = state_legislature_bill.get("sponsorships", [])
        legislation_type = state_legislature_bill.get("classification", [])
        subjects = state_legislature_bill.get("subject", [])
        latest_action_description = state_legislature_bill.get(
            "latest_action_description", ""
        )

        first_action_date = state_legislature_bill.get("first_action_date", "")
        latest_action_date = state_legislature_bill.get("latest_action_date", "")

        (
            sponsor_legislator_list,
            co_sponsor_legislator_list,
        ) = self.get_state_legislators(legislators, state_obj)
        if state_obj:
            state_legislative_session_obj = self.get_legislative_session_obj(state_obj, session)
            state_legislator_bill.update(
                {
                    "bill_id": bill_id,
                    "chamber": chamber,
                    "session": session,
                    "identifier": identifier,
                    "bill_title": bill_title,
                    "state": state_obj,
                    "legislation_type": legislation_type,
                    "subjects": subjects,
                    "latest_action_description": latest_action_description,
                    "bill_json": state_legislature_bill,
                    "legislative_session": state_legislative_session_obj
                }
            )

            if first_action_date:
                action_date = self.get_date_from_datetime(first_action_date)
                action_date = datetime.strptime(str(action_date), "%Y-%m-%d").date()
                state_legislator_bill.update({"introduced_at": action_date})

            if latest_action_date:
                last_action_date = self.get_date_from_datetime(latest_action_date)
                last_action_date = datetime.strptime(str(last_action_date), "%Y-%m-%d").date()
                state_legislator_bill.update({"latest_action_date": last_action_date})

            # bill data save
            state_legislator_bill_obj = StateLegislatorBill.objects.update_or_create(
                bill_id=bill_id, defaults=state_legislator_bill
            )[0]
            if state_legislator_bill_obj:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"\n\nlegislature bill add Bill id -- {state_legislator_bill_obj.id}"
                    )
                )

                # bill sponsors save
                state_legislator_bill_obj.sponsors.set(sponsor_legislator_list)
                if sponsor_legislator_list:
                    self.stdout.write(
                        self.style.WARNING(
                            f"State Bill in Sponsors in legislature added Bill id -- {state_legislator_bill_obj.id}"
                        )
                    )

                # bill co-sponsors save
                state_legislator_bill_obj.co_sponsors.set(co_sponsor_legislator_list)
                if co_sponsor_legislator_list:
                    self.stdout.write(
                        self.style.WARNING(
                            f"State Bill in Co sponsors in legislature added Bill id -- {state_legislator_bill_obj.id}"
                        )
                    )

                # bill actions store
                self.bill_actions_data(
                    state_legislature_bill, state_legislator_bill_obj
                )

                # bill text store
                self.bill_text_data_store(state_legislator_bill_obj)
            else:
                self.stdout.write(self.style.ERROR(f"State Bill not add -- {bill_id}"))
        else:
            self.stdout.write(
                self.style.ERROR(f"legislature state not found -- {state_obj}")
            )
        return None

    def get_api_key(self):
        return StateBillAPIKeyTracker.objects.filter(is_available=True).first()

    def get_state_bill_status(self, jurisdictions_id):
        return StateBillTracker.objects.filter(
            state_jurisdiction_id=jurisdictions_id
        ).exists()

    def state_legislator_bill(self, jurisdiction_id):
        """
        jurisdiction id, api key, page number add dynamically in url
        :param jurisdiction_id:
        :return:
        """
        bill_url = f"https://v3.openstates.org/bills?jurisdiction={jurisdiction_id}&sort=updated_desc&include=sponsorships&include=abstracts&include=other_titles&include=other_identifiers&include=actions&include=sources&include=documents&include=versions&include=votes&page={self.page_num}&per_page=20&apikey={self.api_key}"
        data = requests.get(bill_url)
        time.sleep(20)
        if data.status_code == 200:
            resp = data.json()
            data_contents = resp.get("results", [])
            for data_content in data_contents:
                self.store_state_legislature_bill(data_content)
            max_page = int(resp["pagination"]["max_page"])
            if self.page_num <= max_page:
                if self.page_num == max_page:
                    self.stdout.write(
                        self.style.SUCCESS(f"Last page - {self.page_num}")
                    )
                    self.page_num = 1
                    StateBillTracker.objects.create(
                        state_jurisdiction_id=jurisdiction_id
                    )
                    return False
                else:
                    self.page_num += 1
                    return True
        elif data.status_code == 429:
            self.stdout.write(
                self.style.ERROR(f"ERROR: API key {self.api_key} expired")
            )
            self.api_key_obj.is_available = False
            self.api_key_obj.retired_time = self.today_date
            self.api_key_obj.save()
            self.api_key_obj = self.get_api_key()
            if not self.api_key_obj:
                raise Exception("No API Key is available")
            self.api_key = self.api_key_obj.key
            return True
        else:
            self.stdout.write(
                self.style.ERROR(
                    f"bill data error- data: {data} - BILL_URL: {bill_url}"
                )
            )
            return True

    def state_bill_update(self, jurisdiction_id, state_name, bill_page_no):
        """
        while loop in all jurisdictions state execute
        :param jurisdiction_id:
        :param state_name:
        :return:
        """
        if bill_page_no:
            self.page_num = bill_page_no
        else:
            self.page_num = 1

        while True:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n\n****** Date: {self.today_date} ****** page number: {self.page_num} ****** state name: {state_name if state_name else jurisdiction_id}"
                )
            )
            status = self.state_legislator_bill(jurisdiction_id)
            if not status:
                break



    def handle(self, *args, **options):
        bill_page_no = options.get("bill_page_no")
        created_since_arg = options.get("created_since", "")
        if created_since_arg:
            self.created_since = created_since_arg
        else:
            self.created_since = ""
        self.today_date = datetime.today().date()
        self.api_key_obj = self.get_api_key()
        if not self.api_key_obj:
            raise Exception("No API is available")
        else:
            self.api_key = self.api_key_obj.key
            jurisdiction_id = options.get("jurisdiction_id")
            bill_id = options.get("bill_id")
            if bill_id:
                bill_id_url = f"https://v3.openstates.org/bills/{bill_id}?include=sponsorships&include=abstracts&include=other_titles&include=other_identifiers&include=actions&include=sources&include=documents&include=versions&include=votes&include=related_bills&apikey={self.api_key}"
                data = requests.get(bill_id_url)
                time.sleep(5)
                if data.status_code == 200:
                    resp = data.json()
                    self.store_state_legislature_bill(resp)
            else:
                if jurisdiction_id:
                    self.state_bill_update(jurisdiction_id, "", bill_page_no)
                else:
                    while True:
                        state_jurisdictions_url = f"https://v3.openstates.org/jurisdictions?classification=state&page=1&per_page=52&apikey={self.api_key}"
                        data = requests.get(state_jurisdictions_url)
                        time.sleep(5)
                        if data.status_code == 200:
                            resp = data.json()
                            data_contents = resp.get("results", [])
                            for data_content in data_contents:
                                jurisdiction_id = data_content.get("id")
                                state_name = data_content.get("name", "")
                                if jurisdiction_id:
                                    if not self.get_state_bill_status(jurisdiction_id):
                                        self.state_bill_update(jurisdiction_id, state_name, bill_page_no)
                                    else:
                                        self.stdout.write(
                                            self.style.ERROR(
                                                f"This state already executed name: {state_name}"
                                            )
                                        )
                                else:
                                    self.stdout.write(
                                        self.style.ERROR("jurisdiction id not found.")
                                    )
                            break
                        elif data.status_code == 429:
                            self.stdout.write(
                                self.style.ERROR(f"ERROR: API key {self.api_key} expired")
                            )
                            self.api_key_obj.is_available = False
                            self.api_key_obj.retired_time = self.today_date
                            self.api_key_obj.save()
                            self.api_key_obj = self.get_api_key()
                            if not self.api_key_obj:
                                raise Exception("No API Key is available")
                            self.api_key = self.api_key_obj.key
                            continue
                        else:
                            self.stdout.write(
                                self.style.ERROR(f"jurisdiction data error - {data}")
                            )
                            break

        self.stdout.write(self.style.SUCCESS("Command ran successfully..."))
