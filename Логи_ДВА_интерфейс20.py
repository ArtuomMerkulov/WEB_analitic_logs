import streamlit as st
import datetime
import calendar
import re
import os
import time
import pandas as pd
import altair as alt
from collections import defaultdict
from dateutil.relativedelta import relativedelta

# Конфигурация
LOG_FILE = 'cess_log.txt'

@st.cache_data(ttl=5)
def load_logs():
    """Загрузка логов с отслеживанием изменений файла"""
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as file:
            return [line.strip() for line in file]
    except Exception as e:
        st.error(f"Ошибка чтения файла: {e}")
        return []

def get_file_mtime():
    """Получение времени последнего изменения файла"""
    try:
        return os.path.getmtime(LOG_FILE)
    except FileNotFoundError:
        return 0

def check_for_updates():
    """Проверка обновлений лог-файла"""
    if 'last_mtime' not in st.session_state:
        st.session_state.last_mtime = get_file_mtime()
    
    current_mtime = get_file_mtime()
    if current_mtime != st.session_state.last_mtime:
        st.session_state.last_mtime = current_mtime
        st.cache_data.clear()
        st.experimental_rerun()

def parse_log_entry(log_line):
    """Парсинг логов с кириллицей и сложным форматом"""
    try:
        line = log_line.strip().replace("('", "").replace("')", "")
        
        time_part, data_part = line.split(" - ", 1)
        
        timestamp = datetime.datetime.strptime(time_part.split(',')[0], '%Y-%m-%d %H:%M:%S')
        
        fields = {}
        for item in data_part.split(", '"):
            if ':' not in item:
                continue
            key, value = item.split(':', 1)
            fields[key.strip(" '")] = value.strip().strip("'")
        
        return {
            'timestamp': timestamp,
            'client_ip': fields.get('Client_IP', 'unknown'),
            'client_hostname': fields.get('Client_Hostname', 'unknown'),
            'server': fields.get('Server', 'unknown'),
            'event': fields.get('Event', 'unknown'),
            'project': fields.get('Project', 'unknown'),
            'login': fields.get('Логин', 'unknown'),
            'org_unit': fields.get('Орг_уровень_5', 'unknown'),
            'fullname': fields.get('ФИО', 'unknown')
        }
    except Exception as e:
        st.error(f"Ошибка в строке: {log_line}\nПричина: {str(e)}")
        return None

@st.cache_data(ttl=5)
def process_logs(raw_logs):
    """Обработка сырых логов"""
    parsed = []
    for line in raw_logs:
        entry = parse_log_entry(line)
        if entry:
            entry['date'] = entry['timestamp'].date()
            parsed.append(entry)
    return parsed

def get_departments(logs=None):
    return [
        "Управление 1",
        "Управление 2",
        "Управление 3",
        "Управление 4",
        "Управление 5",
        "Управление 6"
    ]

def assign_departments_to_logs(logs, departments):
    for log in logs:
        try:
            ip = log['client_ip']
            clean_ip = '.'.join([octet for octet in ip.split('.') if octet.isdigit()][:4])
            last_octet = int(clean_ip.split('.')[-1]) if clean_ip.count('.') == 3 else 0
            log['department'] = departments[last_octet % len(departments)]
        except:
            log['department'] = departments[0]
    return logs

def get_month_range(start, end):
    months = []
    current = datetime.date(start.year, start.month, 1)
    end_date = datetime.date(end.year, end.month, 1)
    
    while current <= end_date:
        months.append((current.year, current.month))
        current += relativedelta(months=+1)
    
    return sorted(list(set(months)), key=lambda x: (x[0], x[1]))

def prepare_chart_data(filtered_logs, start_date, end_date):
    """Подготовка данных для графика с динамической группировкой по времени"""
    delta = relativedelta(end_date, start_date)
    total_months = delta.years * 12 + delta.months
    
    # Определение уровня группировки
    if total_months <= 12:
        date_format = "%Y-%m"
        freq = 'month'
    else:
        date_format = "%Y"
        freq = 'year'
    
    data = defaultdict(lambda: defaultdict(int))
    departments = get_departments()
    
    for log in filtered_logs:
        dept = log['department']
        date = log['date']
        
        if freq == 'year':
            period = date.strftime(date_format)
            if date.year == end_date.year and date.month <= end_date.month:
                period = f"{date.year}-{date.month:02d}"
        else:
            period = date.strftime(date_format)
        
        data[dept][period] += 1
    
    df_data = []
    for dept in departments:
        for period, count in data[dept].items():
            df_data.append({
                'Управление': dept,
                'Период': period,
                'Посещения': count
            })
    
    return pd.DataFrame(df_data), freq

def prepare_employee_data(employee_logs, start_date, end_date):
    """Подготовка данных по сотрудникам для графика"""
    delta = relativedelta(end_date, start_date)
    total_months = delta.years * 12 + delta.months
    
    freq = 'month' if total_months <= 12 else 'year'
    date_format = "%Y-%m" if freq == 'month' else "%Y"
    
    df_data = []
    for employee, stats in employee_logs.items():
        for (year, month), count in stats['months'].items():
            date = datetime.date(year, month, 1)
            period = date.strftime(date_format)
            
            if freq == 'year' and date.year == end_date.year and date.month <= end_date.month:
                period = f"{year}-{month:02d}"
                
            df_data.append({
                'Сотрудник': employee,
                'Период': period,
                'Посещения': count
            })
    
    return pd.DataFrame(df_data), freq

def main():
    # Инициализация состояния
    if 'first_run' not in st.session_state:
        st.session_state.first_run = True
        st.session_state.last_mtime = get_file_mtime()

    # Обработка параметров URL
    query_params = st.experimental_get_query_params()
    
    # Получение дат из URL или установка значений по умолчанию
    today = datetime.date.today()
    default_start = today - relativedelta(months=3)
    default_end = today
    
    start_date = default_start
    end_date = default_end
    
    if 'start_date' in query_params:
        try:
            start_date = datetime.date.fromisoformat(query_params['start_date'][0])
        except:
            pass
    
    if 'end_date' in query_params:
        try:
            end_date = datetime.date.fromisoformat(query_params['end_date'][0])
        except:
            pass

    # Проверка обновлений
    check_for_updates()

    # Основной интерфейс
    st.markdown(
        "<h1 style='text-align: center; margin-bottom: 30px;'>Логи портала ДВА</h1>", 
        unsafe_allow_html=True
    )

    # Загрузка и обработка данных
    raw_logs = load_logs()
    parsed_logs = process_logs(raw_logs)
    departments = get_departments()
    parsed_logs = assign_departments_to_logs(parsed_logs, departments)

    # Секция настройки периода
    st.sidebar.header("Настройка периода отображения")
    try:
        selected_range = st.sidebar.date_input(
            "Выберите диапазон (начало и конец)",
            value=(start_date, end_date),
            format="YYYY/MM/DD"
        )
        
        if isinstance(selected_range, (tuple, list)) and len(selected_range) == 2:
            start_date, end_date = selected_range
        else:
            st.sidebar.warning("⚠️ Выберите две даты для формирования периода")

    except Exception as e:
        st.sidebar.error(f"Ошибка ввода дат: {str(e)}. Используется период по умолчанию")
        
    # Комментарий о периоде анализа
    st.sidebar.markdown(
        f"**Подсчет логов (посещений) сайта Портала ДВА ведется с " 
        f"{start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}**"
    )
    
    # Фильтрация логов
    filtered_logs = [log for log in parsed_logs if start_date <= log['date'] <= end_date]

    # Подготовка матрицы данных для управлений
    all_available_months = get_month_range(start_date, end_date)
    display_months = all_available_months
    
    dept_matrix = {dept: {'total': 0, 'months': defaultdict(int)} for dept in departments}
    for log in filtered_logs:
        try:
            dept = log['department']
            year_month = (log['date'].year, log['date'].month)
            dept_matrix[dept]['total'] += 1
            dept_matrix[dept]['months'][year_month] += 1
        except KeyError as e:
            pass

    # Стили таблицы
    st.markdown("""
    <style>
        .scrollable-table {
            max-width: 100%;
            overflow-x: auto;
            margin: 20px 0;
            border: 1px solid #ddd;
            border-radius: 5px;
        }
        
        .fixed-column {
            position: sticky;
            left: 0;
            background: white;
            z-index: 1;
            border-right: 2px solid #ddd;
        }
        
        .month-header {
            min-width: 80px;
            text-align: center !important;
        }
        
        .data-cell {
            text-align: center;
            vertical-align: middle !important;
            border-left: 1px solid #ddd;
            min-width: 80px;
        }
        
        .total-column {
            background-color: #f8f9fa;
            font-weight: bold;
        }
        
        .scrollable-table a {
            color: #1f77b4;
            text-decoration: none;
            font-weight: bold;
        }

        .scrollable-table a:hover {
            text-decoration: underline;
            cursor: pointer;
        }
    </style>
    """, unsafe_allow_html=True)

        # Стили таблицы (оставить без изменений)
    st.markdown("""
    <style>
        .scrollable-table {
            max-width: 100%;
            overflow-x: auto;
            margin: 20px 0;
            border: 1px solid #ddd;
            border-radius: 5px;
        }
        
        .fixed-column {
            position: sticky;
            left: 0;
            background: white;
            z-index: 1;
            border-right: 2px solid #ddd;
        }
        
        .month-header {
            min-width: 80px;
            text-align: center !important;
        }
        
        .data-cell {
            text-align: center;
            vertical-align: middle !important;
            border-left: 1px solid #ddd;
            min-width: 80px;
        }
        
        .total-column {
            background-color: #f8f9fa;
            font-weight: bold;
        }
    </style>
    """, unsafe_allow_html=True)

    # Выбор управления через selectbox
    selected_dept = st.selectbox(
        "Выберите управление для отображения статистики:",
        ["Все управления"] + departments,
        index=0
    )

    if selected_dept == "Все управления":
        # Общая статистика по всем управлениям
        st.markdown("### Общая статистика по управлениям")
        
        # Таблица
        table_html = """
        <div class="scrollable-table">
            <table class="table">
                <thead>
                    <tr>
                        <th class="fixed-column">Управление</th>
                        <th class="total-column">Сумма</th>
                        {}
                    </tr>
                </thead>
                <tbody>
                    {}
                </tbody>
            </table>
        </div>
        """.format(
            "".join([f'<th class="month-header">{calendar.month_abbr[month]} {year}</th>' 
                    for year, month in display_months]),
            
            "".join([
                f'<tr>'
                f'<td class="fixed-column">{dept}</td>'
                f'<td class="total-column">{dept_matrix[dept]["total"]}</td>'
                + "".join([f'<td class="data-cell">{dept_matrix[dept]["months"].get((year, month), 0)}</td>' 
                          for year, month in display_months]) +
                '</tr>'
                for dept in departments
            ])
        )
        st.markdown(table_html, unsafe_allow_html=True)

        # График
        st.markdown("---")
        st.markdown("### График посещений по управлениям")
        chart_df, freq = prepare_chart_data(filtered_logs, start_date, end_date)
        
        color_scheme = [
            '#1f77b4', '#ff7f0e', '#2ca02c',
            '#d62728', '#9467bd', '#8c564b'
        ]
        
        line = alt.Chart(chart_df).mark_line().encode(
            x=alt.X('Период:N', title='Период', axis=alt.Axis(labelAngle=45)),
            y=alt.Y('Посещения:Q', title='Количество посещений'),
            color=alt.Color('Управление:N', 
                          scale=alt.Scale(range=color_scheme),
                          legend=alt.Legend(
                              title="Управления",
                              columns=2,
                              symbolLimit=6
                          ))
        )

        points = alt.Chart(chart_df).mark_point(
            filled=True,
            size=80,
            stroke='white',
            strokeWidth=1,
            opacity=0.8
        ).encode(
            x=alt.X('Период:N'),
            y=alt.Y('Посещения:Q'),
            shape=alt.Shape('Управление:N', legend=None),
            tooltip=['Управление', 'Период', 'Посещения']
        )

        chart = (line + points).properties(
            width=800,
            height=400
        ).interactive()

        st.altair_chart(chart, use_container_width=True)

    else:
    # Детализированная статистика по выбранному управлению
    st.markdown(f"### Детальная статистика по управлению: {selected_dept}")
    
    # Фильтрация логов
    dept_logs = [log for log in filtered_logs 
                if log['department'] == selected_dept]
    
    # Создание матрицы данных для сотрудников
    employee_matrix = defaultdict(lambda: {'total': 0, 'months': defaultdict(int)})
    for log in dept_logs:
        name = log['fullname']
        if name == 'unknown':
            continue
        year_month = (log['date'].year, log['date'].month)
        employee_matrix[name]['total'] += 1
        employee_matrix[name]['months'][year_month] += 1
    
    if not employee_matrix:
        st.warning(f"В управлении '{selected_dept}' нет данных о посещениях за выбранный период")
    else:
        # Таблица сотрудников
        st.markdown("#### Посещения сотрудников")
        employee_table_html = """
        <div class="scrollable-table">
            <table class="table">
                <thead>
                    <tr>
                        <th class="fixed-column">ФИО сотрудника</th>
                        <th class="total-column">Сумма</th>
                        {}
                    </tr>
                </thead>
                <tbody>
                    {}
                </tbody>
            </table>
        </div>
        """.format(
            "".join([f'<th class="month-header">{calendar.month_abbr[month]} {year}</th>' 
                    for year, month in display_months]),
            
            "".join([
                f'<tr>'
                f'<td class="fixed-column">{name}</td>'
                f'<td class="total-column">{stats["total"]}</td>'
                + "".join([f'<td class="data-cell">{stats["months"].get((year, month), 0)}</td>' 
                          for year, month in display_months]) +
                '</tr>'
                for name, stats in employee_matrix.items()
            ])
        )
        st.markdown(employee_table_html, unsafe_allow_html=True)
        
        # График для сотрудников
        st.markdown("---")
        st.markdown("#### График посещений сотрудников")
        chart_df, freq = prepare_employee_data(employee_matrix, start_date, end_date)
        
        if not chart_df.empty:
            line = alt.Chart(chart_df).mark_line().encode(
                x=alt.X('Период:N', title='Период', axis=alt.Axis(labelAngle=45)),
                y=alt.Y('Посещения:Q', title='Количество посещений'),
                color=alt.Color('Сотрудник:N', legend=alt.Legend(title="Сотрудники")),
                tooltip=[
                    alt.Tooltip('Сотрудник:N', title="Сотрудник"),
                    alt.Tooltip('Период:T', title="Период"),
                    alt.Tooltip('Посещения:Q', title="Посещения")
                ]
            )

            points = alt.Chart(chart_df).mark_point(
                filled=True,
                size=80,
                stroke='white',
                strokeWidth=1,
                opacity=0.8
            ).encode(
                x=alt.X('Период:N'),
                y=alt.Y('Посещения:Q'),
                shape=alt.Shape('Сотрудник:N', legend=None),
                tooltip=[
                    alt.Tooltip('Сотрудник:N', title="Сотрудник"),
                    alt.Tooltip('Период:T', title="Период"),
                    alt.Tooltip('Посещения:Q', title="Посещения")
                ]
            )

            chart = (line + points).properties(
                width=800,
                height=400
            ).interactive()

            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Недостаточно данных для построения графика")

if __name__ == "__main__":
    main()