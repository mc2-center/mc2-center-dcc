"""Truncate Synapse team.

This script will remove all team members from a given Synapse team
ID (-t), with the exception of:

  * Ashley Clayton (ashley.clayton, 3408068)
  * Amber Nelson (ambernelson, 3419821)
  * Verena Chung (vchung, 3393723)
"""

import argparse
from typing import List

from synapseclient import Synapse
from synapseclient.models import Table, Team


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(
        description="Removes all non-manager team members from Synapse teams."
    )
    parser.add_argument(
        "-t",
        "--table_id",
        type=str,
        required=True,
        help="Synapse table containing team IDs to truncate.",
    )
    return parser.parse_args()


def truncate_members(syn: Synapse, team_id: str) -> None:
    """Remove all non-manager Synapse users from given team."""

    # Synapse user IDs for Amber, Ashley, and Verena - DO NOT REMOVE FROM TEAM!
    manager_ids = ["3408068", "3419821", "3393723"]

    count = 0
    team_members = Team(id=team_id).members(synapse_client=syn)
    for member in team_members:
        user_id = str(member.member.owner_id)
        if user_id not in manager_ids:
            # No OOP equivalent exists for team-member removal; the Team
            # model has no remove-member method, so this stays a raw REST call.
            syn.restDELETE(f"/team/{team_id}/member/{user_id}")
            count += 1

    # Output mini-summary report.
    team = Team(id=team_id).get(synapse_client=syn)
    print(f"Removed {count} members from team: {team.name}")


def reset_teams(syn: Synapse, teams: List[str]) -> None:
    """Reset teams by removing all non-manager members."""
    for team in teams:
        truncate_members(syn, team)


def get_teams(syn: Synapse, table_id: str) -> List[str]:
    """Return a list of team IDs."""
    return (
        Table(id=table_id)
        .query(query=f"SELECT team_id FROM {table_id}", synapse_client=syn)
        .team_id.tolist()
    )


def main():
    """Main function."""
    syn = Synapse()
    syn.login(silent=True)
    args = get_args()

    teams = get_teams(syn, args.table_id)
    reset_teams(syn, teams)


if __name__ == "__main__":
    main()
