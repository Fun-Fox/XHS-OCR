from core.logger import logger

async def get_user_profile_data(author_profile_url):
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,  # 保持headless模式
        )
        context = await browser.new_context()
        cookie_str = "abRequestId=dbb2a223-61fa-5f7d-b929-9c358ea6d049; a1=19a48741019h08vwqsc9n7135g555j7365gdeh3ht50000230323; webId=bb6a2be9ab3649da1e8a762e4c1eac1d; gid=yj04fiWDDDKWyj04YW4y82xqyjx8YhEAMSjIkx9Wyq2k2f2822VWqd888Jq8qJq84jS8fif0; web_session=0400697664e070a8934007f03c3b4b64708352; customer-sso-sid=68c51756919605332687258275m4jiee22zdldtf; x-user-id-creator.xiaohongshu.com=599bb8fa6a6a693f0c70c316; customerClientId=255154908648415; access-token-creator.xiaohongshu.com=customer.creator.AT-68c517569196053326888962kkyqhmtaobfpjqk3; galaxy_creator_session_id=nyykGgynplQT7inAtHNCai92I4R6idRrD1ts; galaxy.creator.beaker.session.id=1762340789805049445724; xsecappid=xhs-pc-web; webBuild=4.84.4; websectiga=2845367ec3848418062e761c09db7caf0e8b79d132ccdd1a4f8e64a11d0cac0d; sec_poison_id=6be1d465-197c-4821-983f-572d57bfcd4e; loadts=1762498122328"
        if cookie_str:
            # 解析 cookie 字符串为字典
            cookies_dict = {}
            for cookie in cookie_str.split('; '):
                key, value = cookie.split('=', 1)
                cookies_dict[key] = value

            # 转换为 cookie 对象数组
            cookies = [
                {"name": name, "value": value, "domain": ".xiaohongshu.com", "path": "/"}
                for name, value in cookies_dict.items()
            ]
            await context.add_cookies(cookies)
        # 使用 context 变量，例如创建页面
        page = await context.new_page()
        # 这里可以添加获取用户资料的具体逻辑
        try:
            await page.goto("https://www.xiaohongshu.com/user/profile/68cbe46500000000210230ff", timeout=30000)
            await page.wait_for_timeout(5000)

            # 获取用户基本信息
            user_data = await page.evaluate('() => window.__INITIAL_STATE__?.user.userPageData._rawValue')
            user_info = {}
            # if not user_data or len(user_data) == 0 or not user_data['basicInfo'] or user_data[
            #     'basicInfo'] == {}:
            if user_data:
                basicInfo = user_data.get('basicInfo', {})
                # 提取用户信息
                user_info['desc'] = basicInfo.get('desc', '')
                user_info['images'] = basicInfo.get('images', '')
                user_info['nickname'] = basicInfo.get('nickname', '')
                user_info['red_id'] = basicInfo.get('redId', '')

                # 提取互动数据
                interactions = user_data.get('interactions', [])
                user_info['follows'] = next(
                    (item['count'] for item in interactions if item['type'] == 'follows'), 0)
                user_info['fans'] = next((item['count'] for item in interactions if item['type'] == 'fans'), 0)
                user_info['interaction'] = next(
                    (item['count'] for item in interactions if item['type'] == 'interaction'), 0)
                logger.info(f"user_info：\n{user_info}")
        except Exception as e:
            logger.info(f"处理页面时出错: {author_profile_url}, 错误: {str(e)}")

        await browser.close()

        # 函数结束时返回需要的数据
        # 注意：使用 async with 时，browser 会自动关闭
        return user_info
