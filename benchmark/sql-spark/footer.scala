val endTime: Date = new Date()
val elapsed = endTime.getTime - startTime.getTime
val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")
val date = dateFormat.format(elapsed)
