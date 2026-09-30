#!/bin/bash
#SBATCH --partition=              # your partition; add --account / --qos if your site needs them
#SBATCH --nodes=1                 # leave as is
#SBATCH --ntasks=1                # leave as is
#SBATCH --cpus-per-task=          # needs to be a multiple of SUBJECTS_PER_TAR
#SBATCH --mem-per-cpu=5G          # 4-5G per cpu is enough for recon-all
#SBATCH --time=0-23:00:00         # one task runs SUBJECTS_PER_TAR subjects from start to finish
#SBATCH --requeue                 # only matters on preemptible partitions

##########################
# SITE SETTINGS - EDIT
##########################
CONTAINER_CMD=                         # apptainer or singularity, whichever your cluster has
MODULES=()                             # modules to load first, or () if it is already on PATH

DATASET=                               # dataset name: used for the job name and every subfolder
INPUT_TYPE=                            # bids or flat

CONTAINER=

BIDS_ROOT=                             # parent of the dataset folder: the input is BIDS_ROOT/DATASET (read only)
PREPROC_ROOT=                          # everything the pipeline writes goes in here, one subfolder per kind

RECONALL_DIR=                          # optional: existing FreeSurfer subjects dir to reuse finished
                                       # recon-all from; folders must be named exactly like the subject
                                       # ids (sub-X, or sub-X_ses-Y). Leave empty if there is none

SUBJECTS_PER_TAR=                      # subjects per archive and per array task; fixed once a dataset has started (DON'T CHANGE AFTERWARDS)
THROTTLE=                              # most array tasks allowed to run at once

##########################
# DO NOT EDIT BELOW
##########################
PIPELINE_FLAGS=(
    --cortical-measures recon-all after2.curvs after2.mantle after2.shfd
                        after2.lgi after2.gwc after2.myelin mind
    --subcortical-measures subcorticalshape scmind subseg segpve
    --qc-measures euler mriqc fsqc
    --after2-surface retess
    --segpve-surfaces native retess
    --mind-variants nonfiltered filtered
    --resample
    --resample-targets fsaverage6
    --resample-measures recon-all
    --parcellate
    --parcellations HCPMMP1
    --parcellate-measures recon-all
    --zip-contents all
)

if [ "${#MODULES[@]}" -gt 0 ]; then
    for M in "${MODULES[@]}"; do module load "$M"; done
fi

set -e
set -u

if [ -z "$PREPROC_ROOT" ]; then
    echo "PREPROC_ROOT is not set" >&2
    exit 1
fi

WORK_ROOT="$PREPROC_ROOT"/work            # nipype work dirs (deleted for subjects successfully completed)
BATCH_ROOT="$PREPROC_ROOT"/batches        # bookkeeping files: rebuilt every run, cleared once an archive is done
ZIP_ROOT="$PREPROC_ROOT"/zips             # KEEP: the archives to send, never deleted
LOG_ROOT="$PREPROC_ROOT"/logs             # slurm logs (.out and .err files)
APPTAINER_CACHE="$PREPROC_ROOT"/apptainer/cache
APPTAINER_TMP="$PREPROC_ROOT"/apptainer/tmp

BIDS_DIR="$BIDS_ROOT"/"$DATASET"
OUTPUT_DIR="$PREPROC_ROOT"/preproc/"$DATASET"
WORK_DIR="$WORK_ROOT"/"$DATASET"
BATCH_DIR="$BATCH_ROOT"/"$DATASET"
ZIP_DIR="$ZIP_ROOT"/"$DATASET"
LOG_DIR="$LOG_ROOT"/"$DATASET"

JOB_NAME="$DATASET-morphometry"

REUSE_FLAGS=()
REUSE_BINDS=()

if [ -n "$RECONALL_DIR" ]; then
    if [ ! -d "$RECONALL_DIR" ]; then
        echo "RECONALL_DIR is set but does not exist: $RECONALL_DIR" >&2
        exit 1
    fi
    REUSE_FLAGS=(--external-subjects-dir "$RECONALL_DIR")
    REUSE_BINDS=(--bind "$RECONALL_DIR")
fi

export APPTAINER_CACHEDIR="$APPTAINER_CACHE" SINGULARITY_CACHEDIR="$APPTAINER_CACHE"
export APPTAINER_TMPDIR="$APPTAINER_TMP"     SINGULARITY_TMPDIR="$APPTAINER_TMP"
mkdir -p "$APPTAINER_CACHE" "$APPTAINER_TMP"

###################################################
# SUBMITTING - EXECUTED WHEN THE SCRIPT IS EXECUTED
###################################################
if [ -z "${SLURM_ARRAY_TASK_ID:-}" ]; then

    SELF=$(readlink -f "$0")
    DATASET_CSV="$OUTPUT_DIR/morphometry/dataset.csv"

    PURGE_ONLY=no
    RETRY=no

    case "${1:-}" in
        --purge-only) PURGE_ONLY=yes ;;   
        --retry)      RETRY=yes ;;  
    esac

    mkdir -p "$OUTPUT_DIR" "$ZIP_DIR" "$WORK_DIR" "$BATCH_DIR" "$LOG_DIR"

    if [ "$(squeue -u "$USER" --noheader --name="$JOB_NAME" | wc -l)" -gt 0 ]; then
        echo "$JOB_NAME still has tasks queued or running - not submitting"
        echo "wait for the array to end, then run this script again"
        exit 1
    fi

    if ! DRY_OUTPUT=$("$CONTAINER_CMD" run --cleanenv \
            --env PYTHONUNBUFFERED=1 \
            --env PYTHONDONTWRITEBYTECODE=1 \
            --bind "$BIDS_DIR","$OUTPUT_DIR" \
            "$CONTAINER" \
            "$BIDS_DIR" "$OUTPUT_DIR" "$INPUT_TYPE" \
            --only-dataset \
            --subjects-per-tar "$SUBJECTS_PER_TAR" \
            "${PIPELINE_FLAGS[@]}" 2>&1); then
        echo "$DRY_OUTPUT" | tail -20
        echo "dry run failed"
        exit 1
    fi

    ARGS_ID=$(echo "$DRY_OUTPUT" | sed -n 's/^ *options id *: *//p')

    if [ -z "$ARGS_ID" ]; then
        echo "could not read the options id from the dry run"
        exit 1
    fi

    echo "$DRY_OUTPUT" | sed -n 's/^ *subjects *: */subjects : /p'

    rm -f "$BATCH_DIR"/*.members "$BATCH_DIR"/*.all \
          "$BATCH_DIR/groups.list" "$BATCH_DIR/complete.list" \
          "$BATCH_DIR/submit.list" "$BATCH_DIR/failed.list" \
          "$BATCH_DIR/incomplete.list"

    awk -v id="$ARGS_ID" -v dir="$BATCH_DIR" '
        BEGIN { FS = "," }
        NR == 1 {
            for (i = 1; i <= NF; i++) {
                if ($i == "subject_id")     { s = i }
                if ($i == "completion_" id) { c = i }
                if ($i == "tar_" id)        { t = i }
            }
            if (!s || !c || !t) { exit 1 }
            next
        }
        {
            group = $t
            sub(/\.tar\.xz$/, "", group)

            total[group]++
            members[group] = members[group] " " $s

            # every member, so a task can tell when its archive is finished
            print $s > (dir "/" group ".all")

            if ($c == "complete") {
                done[group]++
            } else {
                if (!(group in seen)) {
                    seen[group] = 1
                    print group > (dir "/groups.list")
                }
                print $s > (dir "/" group ".members")
            }
        }
        END {
            # nothing pending: no task will ever rebuild this archive again
            for (g in total)
                if (total[g] == done[g])
                    print g members[g] > (dir "/complete.list")
        }
    ' "$DATASET_CSV" || { echo "no columns for options id $ARGS_ID in $DATASET_CSV"; exit 1; }

    N_SUBJECTS=$(cat "$BATCH_DIR"/*.all | wc -l)

    ##########################
    # PURGE - groups already complete at launch
    ##########################
    if [ -s "$BATCH_DIR/complete.list" ]; then

        while read -r GROUP SUBJECTS; do

            ARCHIVE="$ZIP_DIR/$GROUP.tar.xz"
            MISSING="$ZIP_DIR/missing_files_${GROUP#morphometry_zip_}.txt"
            PURGED="$BATCH_DIR/$GROUP.purged"

            if [ -f "$PURGED" ]; then
                continue
            fi

            if [ ! -f "$ARCHIVE" ]; then
                echo "skip $GROUP: no archive"
                continue
            fi

            if [ -f "$MISSING" ]; then
                echo "skip $GROUP: the pipeline listed missing files"
                continue
            fi

            if ! tar -tJf "$ARCHIVE" > /dev/null 2>&1; then
                echo "skip $GROUP: archive does not read back"
                continue
            fi

            echo "purging $GROUP"

            for SUBJECT in $SUBJECTS; do
                find "$OUTPUT_DIR/morphometry" -mindepth 2 -maxdepth 3 -type d \
                     \( -name "$SUBJECT" -o -name "${SUBJECT}_retess" \) \
                     -not -path "*/logs/*" \
                     -prune -exec rm -rf {} +
            done

            rm -rf "$WORK_DIR/$GROUP"

            touch "$PURGED"
            rm -f "$BATCH_DIR/$GROUP.done"    "$BATCH_DIR/$GROUP.failed" \
                  "$BATCH_DIR/$GROUP.members" "$BATCH_DIR/$GROUP.all"

        done < "$BATCH_DIR/complete.list"
    fi

    if [ "$PURGE_ONLY" = yes ]; then
        echo "purge only - not submitting"
        exit 0
    fi

    ##########################
    # WHAT IS LEFT
    ##########################
    if [ ! -s "$BATCH_DIR/groups.list" ]; then
        echo "FINISHED: all $N_SUBJECTS subject(s) complete"
        echo "next step: send all the files in $ZIP_DIR to"
        echo "  leonardo.saraiva@yale.edu"
        echo "  iss31@cam.ac.uk"
        exit 0
    fi

    # a pending archive is in one of three states:
    #   .failed   the pipeline ran and exited with an error
    #   .done     the pipeline ran without errors, but outputs the archive
    #             expects were never produced (e.g. PIPELINE_FLAGS leaves out
    #             measures), so its subjects can never count as complete
    #   neither   the task was interrupted (preemption, time limit): run again
    : > "$BATCH_DIR/submit.list"
    : > "$BATCH_DIR/failed.list"
    : > "$BATCH_DIR/incomplete.list"

    while read -r GROUP; do
        if [ "$RETRY" = yes ]; then
            echo "$GROUP" >> "$BATCH_DIR/submit.list"
        elif [ -f "$BATCH_DIR/$GROUP.failed" ]; then
            echo "$GROUP" >> "$BATCH_DIR/failed.list"
        elif [ -f "$BATCH_DIR/$GROUP.done" ]; then
            echo "$GROUP" >> "$BATCH_DIR/incomplete.list"
        else
            echo "$GROUP" >> "$BATCH_DIR/submit.list"
        fi
    done < "$BATCH_DIR/groups.list"

    count_members() {
        local n=0 group
        while read -r group; do
            n=$(( n + $(wc -l < "$BATCH_DIR/$group.members") ))
        done < "$1"
        echo "$n"
    }

    N_FAILED=$(count_members "$BATCH_DIR/failed.list")
    N_INCOMPLETE=$(count_members "$BATCH_DIR/incomplete.list")
    N_TODO=$(count_members "$BATCH_DIR/submit.list")
    N_COMPLETE=$(( N_SUBJECTS - N_FAILED - N_INCOMPLETE - N_TODO ))

    if [ ! -s "$BATCH_DIR/submit.list" ]; then

        if [ "$N_FAILED" -gt 0 ]; then
            echo "FINISHED with failures: $N_COMPLETE/$N_SUBJECTS complete, $N_FAILED failed"
            echo ""
            echo "failing subject(s):"
            while read -r GROUP; do
                sed 's/^/  /' "$BATCH_DIR/$GROUP.members"
            done < "$BATCH_DIR/failed.list"
            echo ""
            if [ "$N_INCOMPLETE" -gt 0 ]; then
                echo "$N_INCOMPLETE other subject(s) ran without errors but with incomplete outputs"
                echo ""
            fi
            echo "running this script again will not change anything"
            echo "next step: send all the files in $ZIP_DIR, plus"
            echo "  $LOG_DIR/preproc_*.err"
            echo "  $OUTPUT_DIR/morphometry/logs/errors"
            echo "to"
            echo "  leonardo.saraiva@yale.edu"
            echo "  iss31@cam.ac.uk"
            echo "or fix the cause yourself and re-run with --retry"
            exit 1
        fi

        echo "FINISHED with incomplete outputs: $((N_COMPLETE + N_INCOMPLETE))/$N_SUBJECTS subject(s) ran without errors, $N_INCOMPLETE of them with outputs missing"
        echo "running this script again will not change anything"
        echo "next step: send all the files in $ZIP_DIR to"
        echo "  leonardo.saraiva@yale.edu"
        echo "  iss31@cam.ac.uk"
        exit 0
    fi

    N_GROUPS=$(wc -l < "$BATCH_DIR/submit.list")

    while read -r GROUP; do
        rm -f "$BATCH_DIR/$GROUP.done" "$BATCH_DIR/$GROUP.failed"
    done < "$BATCH_DIR/submit.list"

    echo "$N_COMPLETE/$N_SUBJECTS complete, $N_TODO to run in $N_GROUPS archive(s)"

    if [ "$N_FAILED" -gt 0 ]; then
        echo "$N_FAILED subject(s) failed previously and are being skipped; --retry includes them"
    fi

    if [ "$N_INCOMPLETE" -gt 0 ]; then
        echo "$N_INCOMPLETE subject(s) already ran without errors but with incomplete outputs and are being skipped; --retry includes them"
    fi

    MAX_ARRAY=$(scontrol show config 2>/dev/null | awk '/^MaxArraySize/ {print $3}')

    if [ -n "${MAX_ARRAY:-}" ] && [ "$N_GROUPS" -ge "$MAX_ARRAY" ]; then
        echo "$N_GROUPS archives exceeds MaxArraySize $MAX_ARRAY - raise SUBJECTS_PER_TAR"
        exit 1
    fi

    ARRAY_JOB=$(sbatch --parsable \
           --job-name="$JOB_NAME" \
           --chdir="$LOG_DIR" \
           --array="1-$N_GROUPS%$THROTTLE" \
           --output="$LOG_DIR/preproc_%A_%a.out" \
           --error="$LOG_DIR/preproc_%A_%a.err" \
           "$SELF" "$ARGS_ID")

    echo "submitted array $ARRAY_JOB"
    echo "when it has finished, run this script again"

    sbatch --job-name="$DATASET-purge" \
           --dependency=afterany:"$ARRAY_JOB" \
           --chdir="$LOG_DIR" \
           --nodes=1 --ntasks=1 --cpus-per-task=1 --mem-per-cpu=8G \
           --time=0-04:00:00 \
           --output="$LOG_DIR/purge_%A.out" \
           --error="$LOG_DIR/purge_%A.err" \
           "$SELF" --purge-only

    exit 0
fi

###################################################
# PROCESSING - ONE ARRAY TASK
###################################################
ARGS_ID=${1:-}

if [ -z "$ARGS_ID" ]; then
    echo "no options id passed to task $SLURM_ARRAY_TASK_ID" >&2
    exit 1
fi

GROUP=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$BATCH_DIR/submit.list")

if [ -z "$GROUP" ]; then
    echo "no archive listed for task $SLURM_ARRAY_TASK_ID" >&2
    exit 1
fi

readarray -t PARTICIPANT_BATCH < "$BATCH_DIR/$GROUP.members"

TASK_WORK_DIR="$WORK_DIR/$GROUP"
JOB_TMP="$TASK_WORK_DIR/tmp"
mkdir -p "$TASK_WORK_DIR" "$JOB_TMP/mpl"

echo "dataset      : $DATASET"
echo "array task   : $SLURM_ARRAY_TASK_ID"
echo "archive      : $GROUP.tar.xz"
echo "participants : ${PARTICIPANT_BATCH[*]}"
echo "work dir     : $TASK_WORK_DIR"
echo "zip dir      : $ZIP_DIR"
echo "cpus         : $SLURM_CPUS_PER_TASK"

if "$CONTAINER_CMD" run --cleanenv \
       --env PYTHONUNBUFFERED=1 \
       --env PYTHONDONTWRITEBYTECODE=1 \
       --env OMP_NUM_THREADS=1 \
       --env ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS=1 \
       --env MPLCONFIGDIR=/tmp/mpl \
       --bind "$JOB_TMP:/tmp" \
       --bind "$BIDS_DIR","$OUTPUT_DIR","$ZIP_DIR","$TASK_WORK_DIR" \
       ${REUSE_BINDS[@]+"${REUSE_BINDS[@]}"} \
       "$CONTAINER" \
       "$BIDS_DIR" "$OUTPUT_DIR" "$INPUT_TYPE" \
       --work-dir "$TASK_WORK_DIR" \
       --zip-dir "$ZIP_DIR" \
       --n-procs "$SLURM_CPUS_PER_TASK" \
       --subjects-per-tar "$SUBJECTS_PER_TAR" \
       --participant-label "${PARTICIPANT_BATCH[@]}" \
       ${REUSE_FLAGS[@]+"${REUSE_FLAGS[@]}"} \
       "${PIPELINE_FLAGS[@]}"
then
    echo "DONE   $DATASET task $SLURM_ARRAY_TASK_ID ($GROUP)"

    touch "$BATCH_DIR/$GROUP.done"

    ##########################
    # PURGE - this task's own archive
    ##########################
    ALL_COMPLETE=yes

    while read -r SUBJECT; do
        if [ ! -f "$OUTPUT_DIR/morphometry/logs/$SUBJECT/zip_complete_$ARGS_ID" ]; then
            ALL_COMPLETE=no
        fi
    done < "$BATCH_DIR/$GROUP.all"

    ARCHIVE="$ZIP_DIR/$GROUP.tar.xz"
    MISSING="$ZIP_DIR/missing_files_${GROUP#morphometry_zip_}.txt"

    if [ "$ALL_COMPLETE" = yes ] && [ -f "$ARCHIVE" ] && [ ! -f "$MISSING" ] \
       && tar -tJf "$ARCHIVE" > /dev/null 2>&1; then

        echo "purging $GROUP"

        while read -r SUBJECT; do
            find "$OUTPUT_DIR/morphometry" -mindepth 2 -maxdepth 3 -type d \
                 \( -name "$SUBJECT" -o -name "${SUBJECT}_retess" \) \
                 -not -path "*/logs/*" \
                 -prune -exec rm -rf {} +
        done < "$BATCH_DIR/$GROUP.all"

        touch "$BATCH_DIR/$GROUP.purged"
        rm -f "$BATCH_DIR/$GROUP.done"    "$BATCH_DIR/$GROUP.failed" \
              "$BATCH_DIR/$GROUP.members" "$BATCH_DIR/$GROUP.all"

        cd "$LOG_DIR"
        rm -rf "$TASK_WORK_DIR"
    else
        echo "not purging $GROUP: group incomplete or archive not verified"
    fi
else
    echo "FAILED $DATASET task $SLURM_ARRAY_TASK_ID ($GROUP)" >&2
    touch "$BATCH_DIR/$GROUP.failed"
    exit 1
fi