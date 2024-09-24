import datetime
import sys
import os

if __name__ == "__main__":
    dt = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')

    date_range = (dt - datetime.timedelta(days=delta) for delta in range(1, 8))

    users_actions = {}
    for date in date_range:
        filepath = os.path.join(os.getcwd(), 'input', f"{date.strftime('%Y-%m-%d')}.csv")
        with open(filepath) as input_csv:
            for string in input_csv.readlines():
                email, act, trash = string.split(',')
                if email not in users_actions:
                    users_actions[email] = {'CREATE': 0,
                                            'READ': 0,
                                            'UPDATE': 0,
                                            'DELETE': 0}
                users_actions[email][act] += 1

    if not os.path.exists('../output'):
        os.makedirs('../output')

    filepath = os.path.join(os.getcwd(), '../output', f"{dt.strftime('%Y-%m-%d')}.csv")
    with open(filepath, 'w') as output_csv:
        for email, values in users_actions.items():
            output_csv.write(','.join([email] + [str(count) for count in values.values()]) + '\n')