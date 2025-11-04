async def get_user_profile_data(author_profile_url):
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        # 使用 context 变量，例如创建页面
        page = await context.new_page()
        # 这里可以添加获取用户资料的具体逻辑
        try:
            await page.goto(author_profile_url, timeout=30000)
            await page.wait_for_timeout(5000)  # 等待页面加载

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
                print(f"user_info：\n{user_info}")
        except Exception as e:
            print(f"处理页面时出错: {author_profile_url}, 错误: {str(e)}")

        await browser.close()

        # 函数结束时返回需要的数据
        # 注意：使用 async with 时，browser 会自动关闭
        return user_info