import sys

from lab.phase11 import campaign
from lab.phase11.plan import freeze_plan
from lab.phase11.resources import freeze as freeze_resources
from lab.phase12 import view


def run():
    quality = view.freeze()
    if not quality["body"]["eligible"]:
        return {"state": "WAITING_FOR_QUALIFIED_COMPLETE_HISTORY", "view": quality["id"], "actual_estimator_fits": 0}
    manifest = campaign.freeze_campaign(quality, freeze_resources(), freeze_plan())
    print(f"Phase 12 campaign {manifest['id']}", file=sys.stderr, flush=True)
    return campaign.supervise(manifest)


def resume(identifier):
    path = campaign.campaign_folder(identifier)
    if path.parent.parent != view.ROOT:
        raise ValueError("Select a Phase 12 complete-history campaign ID")
    return campaign.resume(identifier)
