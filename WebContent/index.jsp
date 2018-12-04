<%@ page language="java" contentType="text/html" isELIgnored="false" pageEncoding="UTF-8" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
    <title>千度一下，你就知道</title>
    <meta http-equiv="pragma" content="no-cache">
    <meta http-equiv="cache-control" content="no-cache">
    <meta http-equiv="expires" content="0">
    <meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
    <meta http-equiv="description" content="This is my page">
</head>

<style>
    /* 清除默认样式 */
    * {
        margin: 0; /* 外边距重置为0 */
        padding: 0; /* 内边距重置为0 */
    }

    .search {
        width: 640px;
        margin: 0 auto; /* 元素左右居中 */
        text-align: center; /* 元素内容居中 */
    }

    .search_input {
        width: 538px; /* 设置为538px，需要加上border的左右宽度就为538px */
        height: 36px;
        border: 1px solid #4791ff;
        float: left;
        padding-left: 10px;
        outline: none;
    }

    .search_btn {
        width: 100px;
        height: 36px;
        line-height: 36px; /* 文本垂直居中 */
        text-align: center; /* 文本左右居中 */
        background: #3385ff;
        color: #fff;
        font-size: 15px;
        float: left; /* 左浮动让按钮与输入框在一排显示 */
        cursor: pointer; /* 鼠标移入出现小手效果 */
    }

    p {
        width: 780px;
        display: block;
        margin: 15px auto;
        font-size: 13px;
        color: #333;
    }

    .result {
        font-size: 14px;
        color: #999;
        line-height: 30px;
        width: 780px;
        margin: 0 auto;
        text-align: center;
    }

    .page {
        list-style: none;
        text-align: center;
    }

    .page li {
        display: inline-block;
        width: 34px;
        height: 34px;
        border: 1px solid #e1e2e3;
        cursor: pointer;
        text-align: center;
        line-height: 34px;
        font-size: 12px;
    }

    .page li:hover {
        background: #f2f8ff;
        border: 1px solid #38f;
    }

    .page li.on {
        font-weight: bold;
        border: none;
    }

    .page .last {
        width: 60px;
    }

    .page .next {
        width: 60px;
    }

    .page .next:hover {
        background: #f2f8ff;
        border: 1px solid #38f;
    }
</style>


<body>
<div class="search">
    <!-- logo -->
    <img src="images/homelogo.png" alt="尚搜一下" width="270" height="129" class="logo">
</div>
<form action="search.do" name="searchForm">

    <div class="search">
        <!-- 搜索主体 -->
        <div class="main">
            <input name="num" value="1" type="hidden">
            <!-- 输入框 -->
            <input type="text" name="keywords" value="${keywords}" class="search_input">
            <div class="search_btn" onclick="searchForm.submit()">千度一下</div>
        </div>
    </div>
</form>
<br/>
<br/>

<c:if test="${! empty page.list}">
    <div align="center">
        千度为您找到相关结果约
        <font color="green">${page.rowCount }</font>
        个
    </div>
    <br/>
    <c:forEach items="${page.list}" var="hb">
        <a style=" width: 780px;display: block;margin: 0 auto;" href="${hb.url}">${hb.title}</a>
        <p>${hb.content}</p>
        <p>${hb.url}</p>
    </c:forEach>

    <ul class="page">

        <c:if test="${page.hasPrevious}">
            <a href="search.do?num=${page.previousPageNum}&keywords=${keywords}">
                <li class="last">上一页</li>
            </a>
        </c:if>
        <c:forEach begin="${page.everyPageStart}" end="${page.everyPageEnd}" var="current">
            <c:choose>
                <c:when test="${current eq page.pageNum}">
                    <li class="on"><a>${current}</a></li>
                </c:when>
                <c:otherwise>
                    <a href="search.do?num=${current}&keywords=${keywords}">
                        <li>${current}</li>
                    </a>
                </c:otherwise>
            </c:choose>

        </c:forEach>
        <c:if test="${page.hasNext}">
            <a href="search.do?num=${page.nextPageNum}&keywords=${keywords}">
                <li class="next">下一页</li>
            </a>
        </c:if>
    </ul>
</c:if>


</body>
</html>
