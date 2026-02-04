function getStartTime(val) {

    let date = new Date();
    if(val == '上一天'){
        date.setDate(date.getDate()-1);
        return formatDateToString(date,true) + " 00:00:00";
    }
    if(val == '上两天'){
        date.setDate(date.getDate()-2);
        return formatDateToString(date,true) + " 00:00:00";
    }
    if(val == '上三天'){
        date.setDate(date.getDate()-3);
        return formatDateToString(date,true) + " 00:00:00";
    }
    if(val=='自定义' || val=='今天')
        return formatDateToString(date,true) + " 00:00:00";
    date.setHours(date.getHours() - val);
    return formatDateToString(date) ;
}
function getEndTime(val) {
    let date = new Date();
    let newEndTime = "";
    if(val == '上一天'){
        date.setDate(date.getDate()-1);
        return formatDateToString(date,true) + " 23:59:59";
    }
    if(val == '上两天'){
        date.setDate(date.getDate()-2);
        return formatDateToString(date,true) + " 23:59:59";
    }
    if(val == '上三天'){
        date.setDate(date.getDate()-3);
        return formatDateToString(date,true) + " 23:59:59";
    }
    if (val == '自定义' || val=='今天'){
        return formatDateToString(date,true) + " 23:59:59";
    }
    return formatDateToString(date);
}

function formatDateToString(date, onlyDay) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    const formattedDate = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
      if (onlyDay) {
        return `${year}-${month}-${day}`
      }
      return formattedDate
}

function formatweekToLabel(days) {
    const dayEnum = {
      1: '周一',
      2: '周二',
      3: '周三',
      4: '周四',
      5: '周五',
      6: '周六',
      7: '周日'
    }
    days = days.sort()
    const start = []
    const end = []
    var reload = true
    for (let index = 0; index < days.length; index++) {
      const element = days[index]
      const element1 = days[index + 1]
      if (reload) {
        start.push(element)
      }

      if (element1 - element === 1) {
        reload = false
        continue
      } else {
        reload = true
        end.push(element)
      }
    }
    var resultMsg = []
    for (let index1 = 0; index1 < start.length; index1++) {
      const startItem = start[index1]
      const endItem = end[index1]
      if (endItem - startItem === 0) {
        resultMsg.push(dayEnum[startItem])
      } else if (endItem - startItem === 1) {
        resultMsg.push(dayEnum[startItem], dayEnum[endItem])
      } else {
        resultMsg.push(dayEnum[startItem] + '至' + dayEnum[endItem])
      }
    }
    return resultMsg.join('、')
  }
