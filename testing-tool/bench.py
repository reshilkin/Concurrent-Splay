import argparse
import json
import os
import subprocess
import sys
import tempfile


def run_and_parse(args):
    with tempfile.NamedTemporaryFile(mode='w+') as temp_file:
        os.chdir(os.path.join(args["repo_dir"], "java"))

        command = [
            'numactl', '--interleave=all', '-C', f'0-{args["thread_num"]-1}',
            'java', '-Xmx32G', f'-DTHREAD_NUM={args["thread_num"]}', f'-DK1={args["k1"]}', '-cp', 'src:lib/gson-2.13.1.jar:lib/deuceAgent-1.3.0.jar', 'contention.benchmark.Test',
            '-ds', f'trees.lockbased.{args["class_name"]}',
            '-json-file', f'{args["launch_config_path"]}',
            '-result-file', f'{temp_file.name}'
        ]

        try:
            # subprocess.run(command, check=True, timeout = 60, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            subprocess.run(command, check=True, timeout=120)
        except subprocess.TimeoutExpired as e:
            print(f"\t\tПроцесс был прерван по таймауту")

        res = json.load(temp_file)

    return res


# stats = ["throughput", "rotations", "traversal", "conflicts", "insertTraversal", "deleteTraversal", "realNodesDeleted"]
stats = ["throughput", "rotations", "traversal", "conflicts"]


def run_once(args, config, launch_config):
    with open(args["launch_config_path"], 'w') as f:
        json.dump(launch_config, f, indent=4)

    results = {name: [] for name in stats}

    print(f"Running {args['class_name']}, ", end='')
    print(f"distribution={args['distribution']}, ", end='')
    print(f"thread_num={args['thread_num']}, ", end='')
    print(f"inv_splay_prob={args['inv_splay_prob']}, ", end='')
    print(f"max_depth={args['max_depth']}, ", end='')
    print(f"k1={args['k1']}, ", end='')
    print(f"k2={args['k2']}")

    for iterations in range(config["iterations"]):
        print(f"\titeration {iterations + 1}")
        res = run_and_parse(args)

        results["throughput"].append(res[0]["throughput"])
        results["rotations"].append(res[0]["commonStatistic"]["structMods"] / (res[0]["throughput"] * 10))
        if res[0]["commonStatistic"]["getCount"] != 0:
            results["traversal"].append(res[0]["commonStatistic"]["nodesTraversed"] / res[0]["commonStatistic"]["getCount"])
            results["conflicts"].append(res[0]["commonStatistic"]["failedLockAcquire"] / res[0]["commonStatistic"]["getCount"])
        else:
            results["traversal"].append(0)
            results["conflicts"].append(0)

        # if res[0]["commonStatistic"]["realNodesDeleted"] != 0:
        #     results["realNodesDeleted"].append(res[0]["commonStatistic"]["realNodesDeleted"])
        # else:
        #     results["realNodesDeleted"].append(0)

        # if res[0]["commonStatistic"]["memoryMeasures"] != 0:
        #     results["memory"].append(res[0]["commonStatistic"]["memory"] / res[0]["commonStatistic"]["memoryMeasures"])
        # else:
        #     results["memory"].append(0)
        
        # if res[0]["commonStatistic"]["numAdd"] != 0:
        #     results["insertTraversal"].append(res[0]["commonStatistic"]["insertNodesTraversed"] / res[0]["commonStatistic"]["numAdd"])
        # else:
        #     results["insertTraversal"].append(0)

        # if res[0]["commonStatistic"]["numRemove"] != 0:
        #     results["deleteTraversal"].append(res[0]["commonStatistic"]["deleteNodesTraversed"] / res[0]["commonStatistic"]["numRemove"])
        # else:
        #     results["deleteTraversal"].append(0)


    with open(config["result"], 'a') as f:
        comment = f"splay_prob_inv={args['inv_splay_prob']}"
        comment = comment + f"&max_depth={args['max_depth']}"
        comment = comment + f"&k1={args['k1']}"
        comment = comment + f"&k2={args['k2']}"
        f.write(f"{args['class_name']}; {args['distribution']}; {args['thread_num']}; {args['size']}; {args['fill']}; {args['rw']}; {args['k1']}; {args['k2']}; {comment}; ")
        for stat in stats:
            f.write(f"{sum(results[stat]) / len(results[stat])}; ")
        f.write("\n")


def iterate_k2(args, config, launch_config):
    for k2 in config["k2"]:
        args["k2"] = k2
        os.environ["K2"] = str(k2)
        run_once(args, config, launch_config)


def iterate_k1(args, config, launch_config):
    for k1 in config["k1"]:
        args["k1"] = k1
        os.environ["K1"] = str(k1)
        iterate_k2(args, config, launch_config)


def iterate_max_depth(args, config, launch_config):
    for max_depth in config["max_depth"]:
        args["max_depth"] = max_depth
        os.environ["MAX_DEPTH"] = str(max_depth)
        iterate_k1(args, config, launch_config)


def iterate_splay_prob(args, config, launch_config):
    for inv_splay_prob in config["inv_splay_prob"]:
        args["inv_splay_prob"] = inv_splay_prob
        os.environ["INV_SPLAY_PROB"] = str(inv_splay_prob)
        
        iterate_max_depth(args, config, launch_config)


def iterate_threads(args, config, launch_config):
    for rng in config["threads"].split(","):
        if "-" in rng:
            a, b = map(int, rng.split('-'))
        else:
            b = a = int(rng)
        for thread_num in range(a, b + 1):
            args["thread_num"] = thread_num
            os.environ["THREAD_NUM"] = str(thread_num)

            launch_config["test"]["numThreads"] = thread_num
            for builder in launch_config["test"]["threadLoopBuilders"]:
                builder["pin"] = list(range(thread_num))
                builder["quantity"] = thread_num

            launch_config["warmUp"]["numThreads"] = thread_num
            launch_config["warmUp"]["threadLoopBuilders"] = launch_config["test"]["threadLoopBuilders"]

            iterate_splay_prob(args, config, launch_config)


def iterate_distributions(args, config):
    for distribution in config["distribution"]:
        args["distribution"] = distribution
        args["launch_config_path"] = os.path.join(config["config_dir"], f"{args['distribution']}.json")
        with open(args["launch_config_path"], 'r') as f:
            launch_config = json.load(f)
        args["size"] = config["size"]
        args["fill"] = config["fill"]
        args["rw"] = config["rw"]
        launch_config["prefill"]["stopCondition"]["commonOperationLimit"] = int(args["fill"] * args["size"])
        launch_config["prefill"]["threadLoopBuilders"][0]["threadLoopBuilder"]["argsGeneratorBuilder"]["waveSize"] = float(args["fill"])
        launch_config["range"] = args["size"]
        for builder in launch_config["test"]["threadLoopBuilders"]:
            builder["threadLoopBuilder"]["parameters"]["insertRatio"] = args["rw"]
            builder["threadLoopBuilder"]["parameters"]["removeRatio"] = args["rw"]
        launch_config["warmUp"]["stopCondition"]["workTime"] = 10000
        launch_config["test"]["stopCondition"]["workTime"] = 20000
        iterate_threads(args, config, launch_config)


def main():
    os.environ["JAVA_HOME"] = "/home/vaksenov/reshilkin/jdk-17.0.12"

    # command = 'find /home/vaksenov/.gradle -type f -name "*.lock" -delete'.split()
    # subprocess.run(command)

    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", help="Path to configuration file (JSON)")

    with open(parser.parse_args().config_path, 'r') as f:
        config = json.load(f)

    for class_name in config["class_name"]:
        args = {"class_name": class_name}
        args["repo_dir"] = config["repo_dir"]

        compile_command = ['javac', '-nowarn', '--add-exports', 'java.base/jdk.internal.vm.annotation=ALL-UNNAMED', '-cp', 'src:lib/gson-2.13.1.jar:lib/deuceAgent-1.3.0.jar:lib/junit.jar', '@sources.txt']
        os.chdir(os.path.join(args["repo_dir"], "java"))
        subprocess.run(compile_command, check=True)

        iterate_distributions(args, config)

if __name__ == "__main__":
    main()
