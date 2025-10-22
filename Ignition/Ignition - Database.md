

```Python
sasName = 'PowerBI_OEEDowntimes'
TABLE_NAME = 'OEEDowntimes'
DW_DATABASE = 'IBY_IGN_PowerBI'
LAST_SCAN_TAG_PATH = '[default]Analysis_Generator/IBY/LAST_SCAN'
IF_INITIALIZED_PATH = '[default]Analysis_Generator/IBY/IF_INITIALIZED'

lines_to_analyze = [
	r"Enterprise\Irwindale\Canning\C01",
	r"Enterprise\Irwindale\Canning\C02",
	r"Enterprise\Irwindale\Canning\C06",
	r"Enterprise\Irwindale\Canning\C15",
	r"Enterprise\Irwindale\Canning\VP"
]

logger = system.util.getLogger('OEE_PowerBI')

initialized = system.tag.readBlocking([IF_INITIALIZED_PATH])[0].value
if not initialized:
	logger.error('Initialization flag not set.')
else:
	lastScan = system.tag.readBlocking([LAST_SCAN_TAG_PATH])[0].value
	if lastScan is None:
		lastScan = system.date.addHours(system.date.now(), -12)
	now = system.date.now()
	validData = False

	for eqPath in lines_to_analyze:
		params = {'eqPath': eqPath}
		try:
			result = system.mes.analysis.executeAnalysis(lastScan, now, sasName, params)
		except Exception as e:
			logger.error('Analysis failed for ' + eqPath + ': ' + str(e))
			continue
		if result is None:
			continue
		ds = result.getDataset()
		if ds is None or ds.getRowCount() == 0:
			continue

		for r in range(ds.getRowCount()):
			try:
				lineName = eqPath.split('\\')[-1]
				unplanned = float(ds.getValueAt(r, 'Unplanned Downtime') or 0)
				planned = float(ds.getValueAt(r, 'Planned Downtime') or 0)
				origReason = str(ds.getValueAt(r, 'Line Downtime Original Reason') or '')
				splitFlag = bool(ds.getValueAt(r, 'Line Downtime Reason Split'))
				note = str(ds.getValueAt(r, 'Equipment Note') or '')
				reason = str(ds.getValueAt(r, 'Line Downtime Reason') or '')
				duration = float(ds.getValueAt(r, 'Line State Duration') or 0)
				fromTS = ds.getValueAt(r, 'From Time Stamp')
				toTS = ds.getValueAt(r, 'To Time Stamp')
				beginTS = ds.getValueAt(r, 'Line State Event Begin')
				endTS = ds.getValueAt(r, 'Line State Event End')
				countVal = int(ds.getValueAt(r, 'Line Downtime Occurrence Count') or 0)
				lineStateType = str(ds.getValueAt(r, 'Line State Type') or '')
				equipment = str(ds.getValueAt(r, 'Line Downtime Equipment Name') or '')
				productCode = str(ds.getValueAt(r, 'Running Can Size') or '')

				# --- Derive Shift from From Time Stamp ---
				shiftNum = 0
				if fromTS is not None:
					hour = system.date.getHour24(fromTS)
					minute = system.date.getMinute(fromTS)
					totalMins = hour * 60 + minute
					if totalMins >= 390 and totalMins < 870:       # 06:30–14:30
						shiftNum = 1
					elif totalMins >= 870 and totalMins < 1350:    # 14:30–22:30
						shiftNum = 2
					else:                                          # 22:30–06:30
						shiftNum = 3

				# --- Skip "Running" events ---
				if lineStateType.strip().lower() == 'running':
					continue

				# --- Timestamp normalization ---
				fromTSVal = fromTS if fromTS is not None else now
				toTSVal = toTS if toTS is not None else now
				beginKey = beginTS if beginTS is not None else fromTS  # stable key
				if beginKey is None:
					continue  # skip rows with no valid start
				beginVal = beginKey
				endVal = endTS if endTS is not None else now
				filterTime = system.date.format(beginKey, 'HH:mm:ss') if beginKey is not None else system.date.format(now, 'HH:mm:ss')
				filterDate = system.date.format(fromTS, 'yyyy-MM-dd') if fromTS is not None else system.date.format(now, 'yyyy-MM-dd')
				currentDate = system.date.format(now, 'yyyy-MM-dd')
				prevDate = system.date.format(system.date.addDays(now, -1), 'yyyy-MM-dd')

				# --- Check if this (Line, Begin) pair exists ---
				existingDS = system.db.runPrepQuery(
					"SELECT COUNT(*) AS cnt FROM OEEDowntimes WHERE [LineStateEventBegin]=? AND [Line]=?",
					[beginVal, lineName],
					DW_DATABASE
				)
				existing = 0
				if existingDS and len(existingDS) > 0:
					existing = existingDS[0]['cnt']

				# --- Update or Insert ---
				if existing and existing > 0:
					system.db.runPrepUpdate(
						"UPDATE OEEDowntimes SET [LineStateEventEnd]=?, [ToTimeStamp]=? WHERE [LineStateEventBegin]=? AND [Line]=?",
						[toTSVal, toTSVal, beginVal, lineName],
						DW_DATABASE
					)
					logger.info('Updated existing event for ' + lineName + ' started at ' + str(beginVal))
				else:
					insVals = [
						unplanned,
						planned,
						origReason,
						shiftNum,
						productCode,
						splitFlag,
						note,
						lineName,
						equipment,
						beginVal,
						endVal,
						reason,
						duration,
						fromTSVal,
						toTSVal,
						countVal,
						lineStateType,
						currentDate,
						prevDate,
						filterTime,
						filterDate
					]

					system.db.runPrepUpdate(
						'INSERT INTO OEEDowntimes ([UnplannedDowntime],[PlannedDowntime],[Original Reason],[Shift],[Product Code],[LineDowntimeReasonSplit],[Note],[Line],[Equipment],[LineStateEventBegin],[LineStateEventEnd],[Reason],[Downtime],[FromTimeStamp],[ToTimeStamp],[Occurrence Count],[Line State Type],[Current Date],[Previous Date],[Filter Time],[Filter Date]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
						insVals,
						DW_DATABASE
					)
					logger.info('Inserted new event for ' + lineName + ' started at ' + str(beginVal))
				validData = True
			except Exception as e:
				logger.error('Insert failed for ' + eqPath + ' row ' + str(r) + ': ' + str(e))
	if validData:
		system.tag.writeBlocking([LAST_SCAN_TAG_PATH], [now])

```
