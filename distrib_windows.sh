PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run \
    --rm \
    --workdir='/src' \
    -v "${PROJECT_DIRPATH}:/src/" \
    tobix/pywine bash -c "wine pip install -r requirements.txt;
                          wine pip install pyinstaller ;
                          wine pyinstaller main.py \
                               --clean \
                               --name ca_start_batches \
                               --distpath=dist/windows/ \
                               --onefile -y;
                               chown -R ${UID} dist; "