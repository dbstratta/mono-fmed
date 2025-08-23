from pathlib import Path

import pandas as pd


pd.options.mode.copy_on_write = True


def get_project_root() -> Path:
    return Path(__file__).parent.parent


def get_data_path() -> Path:
    return get_project_root() / "data"


def process_requests(requests: pd.DataFrame) -> pd.DataFrame:
    requests = requests.drop(
        # Unnecessary
        columns=["n"]
    ).rename(
        # Cleaner names
        columns={
            "cahashid": "id",
            "Edad": "edad",
            "Sexo": "sexo",
            "AP_CV": "ap_cardiovasculares",
            "zapcardd0": "ap_cardiovascular1",
            "zapcardd1": "ap_cardiovascular2",
            "zapcardd2": "ap_cardiovascular3",
            "zapcardd3": "ap_cardiovascular4",
            "AP_FRCV": "ap_factores_riesgo_cardiovascular",
            "Tabaco": "ap_tabaco",
            "HTA": "ap_hipertension_arterial",
            "Diabetes": "ap_diabetes",
            "Dislipemia": "ap_dislipemia",
            "Obesidad": "ap_obesidad",
            "eecg": "tiene_ecg_previo",
            "zeecgr0": "ecg_previo_resultado1",
            "zeecgr1": "ecg_previo_resultado2",
            "ehol": "tiene_holter_previo",
            "zeholr0": "holter_previo_resultado1",
            "zeholr1": "holter_previo_resultado2",
            "diag_1": "diagnostico1",
            "diag_2": "diagnostico2",
            "Origen_pa": "origen_paciente",
            "IMAE": "imae",
        },
    )

    requests = requests[
        requests["id"].notna()
        & requests["sexo"].isin(["M", "F"])
        & requests["edad"].notna()
    ]

    requests["imae"] = requests["imae"].map(
        {"MVD": "Montevideo", "INTERIOR": "Interior"}
    )

    requests["diagnostico_principal_agrupado"] = requests["diagnostico1"].map(
        {
            "BAVC": "Bloqueo AV",
            "BAV 2do": "Bloqueo AV",
            "BAV 1er": "Bloqueo AV",
            "BAVC-Post cirugia cardiaca": "Bloqueo AV",
            "BAV post ablacion": "Bloqueo AV",
            "BAVC-Post IAM": "Bloqueo AV",
            "DNS - Fibrilacion auricular cronica + bradicardia": "Disfunción del nodo sinusal",
            "DNS - Bradicardia": "Disfunción del nodo sinusal",
            "DNS - Sindrome bradicardia-taquicardia": "Disfunción del nodo sinusal",
            "Pausa sinusal": "Disfunción del nodo sinusal",
            "Bloqueo de ramas - todas las combinaciones": "Bloqueo de rama",
            "Sincope del seno carotideo": "Síncope",
            "Sincope neurocardiogenico": "Síncope",
            "Ritmos ventriculares - Torsade de Pointes/ Sind. de QT prolongad": "Ritmos ventriculares",
            "Ritmos ventriculares - Fibrilacion ventricular paroxistica": "Ritmos ventriculares",
            "Taquicardia ventricular": "Ritmos ventriculares",
            "Cardiomiopatia congestiva": "Cardiomiopatía",
            "Cardiomiopatia hipertrofica": "Cardiomiopatía",
            "Ritmo sinusal normal": "Ritmo sinusal normal",
            "RNS + estudio electrofisiologico anormal": "Estudio electrofisiólogico anormal",
        }
    )

    for column in [
        "ap_cardiovasculares",
        "ap_tabaco",
        "ap_diabetes",
        "ap_dislipemia",
        "ap_hipertension_arterial",
        "ap_obesidad",
        "tiene_ecg_previo",
        "tiene_holter_previo",
    ]:
        requests[column] = clean_bool(requests[column])

    return requests


def process_pacemakers(pacemakers: pd.DataFrame) -> pd.DataFrame:
    pacemakers = pacemakers.drop(
        # Unnecessary
        columns=["n"],
    ).rename(
        # Cleaner names
        columns={
            "cahashid": "id",
            "zcasinst": "imae_implante",
            "caorigen": "tipo_imae_implante",
            "oportunidad": "oportunidad_implante",
            "vivo": "vivo_al_alta",
            "vía_abordaje": "via_de_abordaje",
            "uso_intro": "uso_de_introductor",
            "modo": "modo_de_marcapasos",
            "comp": "hubo_complicaciones",
            "cafecrea": "fecha_realizacion",
            "año": "anio_realizacion",
            "cual_comp1": "complicacion1",
            "cual_comp2": "complicacion2",
        },
    )
    pacemakers = pacemakers[~pacemakers["id"].isna()]

    pacemakers["vivo_al_alta"] = pacemakers["vivo_al_alta"].map(
        {"Vivo": "S", "Fallecido": "N"}
    )

    pacemakers["tipo_imae_implante"] = pacemakers["tipo_imae_implante"].map(
        {"PRI": "privado", "PUBLICO": "publico"}
    )

    for column in ["uso_de_introductor", "hubo_complicaciones"]:
        pacemakers[column] = clean_bool(pacemakers[column])

    return pacemakers


def merge_dataframes(requests: pd.DataFrame, pacemakers: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(
        requests,
        pacemakers,
        on="id",
        how="inner",
        suffixes=(None, None),
    )


def process_merged(merged: pd.DataFrame) -> pd.DataFrame:
    merged = merged.drop_duplicates(subset=["id"])

    return merged


def merge_columns(merged: pd.DataFrame, column_base: str, n: int) -> pd.DataFrame:
    dfs = []

    for i in range(1, n + 1):
        df = (
            merged[
                [
                    "id",
                    "sexo",
                    "tipo_imae_implante",
                    f"{column_base}{i}",
                ]
            ]
            .copy()
            .rename(columns={f"{column_base}{i}": column_base})
        )
        df = df[df[column_base].notna()]

        dfs.append(df)

    cardiovascular_history_df = pd.concat(dfs).sort_values(by=column_base)

    return cardiovascular_history_df


def merge_columns_and_write(
    merged: pd.DataFrame,
    column_base: str,
    n: int,
    data_path: Path,
    file_name: str,
    map_dict=None,
):
    merged_columns = merge_columns(merged, column_base, n)

    if map_dict:
        merged_columns[column_base] = merged_columns[column_base].map(map_dict)

    merged_columns.to_csv(f"{data_path}/{file_name}", index=False, sep="\t")


def clean_bool(series: pd.Series) -> pd.Series:
    return series.map({0: "N", 1: "S", pd.NA: "N", "S": "S", "N": "N"})


def clean() -> None:
    data_path = get_data_path()
    requests = pd.read_excel(f"{data_path}/requests.xls")
    pacemakers = pd.read_excel(f"{data_path}/pacemakers.xls", parse_dates=["cafecrea"])

    requests = process_requests(requests)
    pacemakers = process_pacemakers(pacemakers)

    merged = merge_dataframes(requests, pacemakers)
    merged = process_merged(merged)

    merge_columns_and_write(
        merged,
        "ap_cardiovascular",
        4,
        data_path,
        "cardiovascular_history.tsv",
        {
            "Insuficiencia cardiaca clase 1": "Insuficiencia cardíaca",
            "Insuficiencia cardiaca clase 2": "Insuficiencia cardíaca",
            "Insuficiencia cardiaca clase 3": "Insuficiencia cardíaca",
            "Insuficiencia cardiaca clase 4": "Insuficiencia cardíaca",
            "Otros antecedentes cardiovasculares": "Otros",
            "Muerte subita": "Otros",
            "Fiebre reumatica": "Otros",
            "Aneurisma aortico": "Otros",
            "Fibrilacion ventricular": "Fibrilación ventricular",
            "Arritmia": "Arritmia",
            "Arritmia- Otras": "Arritmia",
            "Sincope": "Síncope",
            "Cardiopatia valvular": "Cardiopatía valvular",
            "Cardiopatia isquemica asintomatica": "Cardiopatía isquémica",
            "Infarto de miocardio": "Cardiopatía isquémica",
            "Cardiopatia congenita": "Cardiopatía congénita",
            "Fibrilacion auricular": "Fibrilación auricular",
            "Taquicardia ventricular": "Taquicardia ventricular",
            "Arritmia- BAV completo": "Bloqueo AV",
        },
    )

    merged.to_csv(f"{data_path}/cleaned.tsv", index=False, sep="\t")


if __name__ == "__main__":
    clean()
