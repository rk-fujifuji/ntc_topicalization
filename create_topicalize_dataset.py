import json
import argparse
from os import path
from itertools import groupby, islice


def create_parser():
    parser = argparse.ArgumentParser(description='Create ntc_topicalization dataset.')
    parser.add_argument('--ntc_dir', type=path.abspath, required=True,
                        help='Directory name which contains ntc data set.')
    return parser


def read_json(file):
    for line in file:
        yield json.loads(line)


def read_ntc(args, file_path):
    for is_eos, sentence in groupby(open(path.join(args.ntc_dir, file_path)), key=lambda x: x.startswith('EOS')):
        if not is_eos:
            sent = ''
            for line in [l.rstrip() for l in sentence]:
                if line.startswith('#') or line.startswith('*'):
                    pass
                else:
                    sent += line.split('\t')[0]
            yield sent


def main():
    parser = create_parser()
    args = parser.parse_args()

    for line in read_json(open('metadata.jsonl')):
        sentences = [sent for sent in islice(read_ntc(args, line['file_path']), line['sent_idx'])]
        context = sentences[:-1]
        gold = sentences[-1]
        ga_gold = gold[line['ga_idx']]

        if ga_gold == 'は':
            s_ha = gold
            s_ga = gold[:line['ga_idx']] + 'が' + gold[line['ga_idx']+1:]
        else:
            s_ha = gold[:line['ga_idx']] + 'は' + gold[line['ga_idx']+1:]
            s_ga = gold

        output = json.dumps(
            {
                'context': context,
                's_ha': s_ha,
                's_ga': s_ga,
                'ga_gold': ga_gold,
                'is_old_info': line['is_old_info'],
                'ans_wo': line['ans_wo'],
                'ans_w': line['ans_w']
            }
        )
        print(output)


if __name__ == '__main__':
    main()