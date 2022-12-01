"""Truncate Synapse team.

This script will remove all team members from a given Synapse team
ID (-t), with the exception of:

  * Julie Bletz (jbletz, 3361792)
  * Ashley Clayton (ashley.clayton, 3408068)
  * Amber Nelson (ambernelson, 3419821)
  * Verena Chung (vchung, 3393723)
"""
import argparse

import synapseclient


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(
        description="Removes all non-manager team members from Synapse teams.")
    parser.add_argument("-t", "--table_id",
                        type=str, required=True,
                        help="Synapse table containing team IDs to truncate.")
    return parser.parse_args()


def truncate_members(syn, team_id):
    """Remove all non-manager Synapse users from given team."""

    # Synapse user IDs for Julie, Amber, Ashley, and Verena - DO NOT REMOVE FROM TEAM!
    manager_ids = ["3361792", "3408068", "3419821", "3393723"]

    count = 0
    team_members = [m.get('member') for m in syn.getTeamMembers(team_id)]
    for user in team_members:
        user_id = user.get('ownerId')
        if user_id not in manager_ids:
            syn.restDELETE(f"/team/{team_id}/member/{user_id}")
            count += 1

    # Output mini-summary report.
    team = syn.getTeam(team_id)
    print(f"Removed {count} members from team: {team.get('name')}")


def reset_teams(syn, teams):
    """Reset teams by removing all non-manager members."""
    for team in teams:
        truncate_members(syn, team)


def get_teams(syn, table_id):
    """Return a list of team IDs."""
    return (
        syn.tableQuery(f"SELECT team_id FROM {table_id}")
        .asDataFrame()
        .team_id
        .tolist()
    )


def main():
    """Main function."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    args = get_args()

    teams = get_teams(syn, args.table_id)
    reset_teams(syn, teams)


if __name__ == "__main__":
    main()
